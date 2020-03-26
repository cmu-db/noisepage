/*

numastat - NUMA monitoring tool to show per-node usage of memory
Copyright (C) 2012 Bill Gray (bgray@redhat.com), Red Hat Inc

numastat is free software; you can redistribute it and/or modify it under the
terms of the GNU Lesser General Public License as published by the Free
Software Foundation; version 2.1.

numastat is distributed in the hope that it will be useful, but WITHOUT ANY
WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
PARTICULAR PURPOSE. See the GNU Lesser General Public License for more details.

You should find a copy of v2.1 of the GNU Lesser General Public License
somewhere on your Linux system; if not, write to the Free Software Foundation,
Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA

*/

/*

Historical note: From approximately 2003 to 2012, numastat was a perl script
written by Andi Kleen to display the /sys/devices/system/node/node<N>/numastat
statistics. In 2012, numastat was rewritten as a C program by Red Hat to
display per-node memory data for applications and the system in general,
while also remaining strictly compatible by default with the original numastat.
A copy of the original numastat perl script is included for reference at the
end of this file.

*/

// Compile with: gcc -O -std=gnu99 -Wall -o numastat numastat.c

#define __USE_MISC
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>

#define STRINGIZE(s) #s
#define STRINGIFY(s) STRINGIZE(s)

#define KILOBYTE (1024)
#define MEGABYTE (1024 * 1024)

#define BUF_SIZE 2048
#define SMALL_BUF_SIZE 128

// Don't assume nodes are sequential or contiguous.
// Need to discover and map node numbers.

int *node_ix_map = NULL;
char **node_header;

// Structure to organize memory info from /proc/<PID>/numa_maps for a specific
// process, or from /sys/devices/system/node/node?/meminfo for system-wide
// data. Tables are defined below for each process and for system-wide data.

typedef struct meminfo {
        int index;
        char *token;
        char *label;
} meminfo_t, *meminfo_p;

#define PROCESS_HUGE_INDEX    0
#define PROCESS_PRIVATE_INDEX 3

meminfo_t process_meminfo[] = {
        { PROCESS_HUGE_INDEX,  "huge", "Huge" },
        {        1,            "heap", "Heap" },
        {        2,            "stack", "Stack" },
        { PROCESS_PRIVATE_INDEX, "N", "Private" }
};

#define PROCESS_MEMINFO_ROWS (sizeof(process_meminfo) / sizeof(process_meminfo[0]))

meminfo_t numastat_meminfo[] = {
        { 0, "numa_hit", "Numa_Hit" },
        { 1, "numa_miss", "Numa_Miss" },
        { 2, "numa_foreign", "Numa_Foreign" },
        { 3, "interleave_hit", "Interleave_Hit" },
        { 4, "local_node", "Local_Node" },
        { 5, "other_node", "Other_Node" },
};

#define NUMASTAT_MEMINFO_ROWS (sizeof(numastat_meminfo) / sizeof(numastat_meminfo[0]))

meminfo_t system_meminfo[] = {
        {  0, "MemTotal", "MemTotal" },
        {  1, "MemFree", "MemFree" },
        {  2, "MemUsed", "MemUsed" },
        {  3, "HighTotal", "HighTotal" },
        {  4, "HighFree", "HighFree" },
        {  5, "LowTotal", "LowTotal" },
        {  6, "LowFree", "LowFree" },
        {  7, "Active", "Active" },
        {  8, "Inactive", "Inactive" },
        {  9, "Active(anon)", "Active(anon)" },
        { 10, "Inactive(anon)", "Inactive(anon)" },
        { 11, "Active(file)", "Active(file)" },
        { 12, "Inactive(file)", "Inactive(file)" },
        { 13, "Unevictable", "Unevictable" },
        { 14, "Mlocked", "Mlocked" },
        { 15, "Dirty", "Dirty" },
        { 16, "Writeback", "Writeback" },
        { 17, "FilePages", "FilePages" },
        { 18, "Mapped", "Mapped" },
        { 19, "AnonPages", "AnonPages" },
        { 20, "Shmem", "Shmem" },
        { 21, "KernelStack", "KernelStack" },
        { 22, "PageTables", "PageTables" },
        { 23, "NFS_Unstable", "NFS_Unstable" },
        { 24, "Bounce", "Bounce" },
        { 25, "WritebackTmp", "WritebackTmp" },
        { 26, "Slab", "Slab" },
        { 27, "SReclaimable", "SReclaimable" },
        { 28, "SUnreclaim", "SUnreclaim" },
        { 29, "AnonHugePages", "AnonHugePages" },
        { 30, "ShmemHugePages", "ShmemHugePages" },
        { 31, "ShmemPmdMapped", "ShmemPmdMapped" },
        { 32, "HugePages_Total", "HugePages_Total" },
        { 33, "HugePages_Free", "HugePages_Free" },
        { 34, "HugePages_Surp", "HugePages_Surp" },
        { 35, "KReclaimable", "KReclaimable" }
};

#define SYSTEM_MEMINFO_ROWS (sizeof(system_meminfo) / sizeof(system_meminfo[0]))

// To allow re-ordering the meminfo memory categories in system_meminfo and
// numastat_meminfo relative to order in /proc, etc., a simple hash index is
// used to look up the meminfo categories. The allocated hash table size must
// be bigger than necessary to reduce collisions (and because these specific
// hash algorithms depend on having some unused buckets.

#define HASH_TABLE_SIZE 151
int hash_collisions = 0;

struct hash_entry {
        char *name;
        int index;
} hash_table[HASH_TABLE_SIZE];

void init_hash_table(void)
{
        memset(hash_table, 0, sizeof(hash_table));
}

int hash_ix(char *s)
{
        unsigned int h = 17;
        while (*s) {
                // h * 33 + *s++
                h = ((h << 5) + h) + *s++;
        }
        return (h % HASH_TABLE_SIZE);
}

int hash_lookup(char *s)
{
        int ix = hash_ix(s);
        while (hash_table[ix].name) {	// Assumes big table with blank entries
                if (!strcmp(s, hash_table[ix].name)) {
                        return hash_table[ix].index;	// found it
                }
                ix += 1;
                if (ix >= HASH_TABLE_SIZE) {
                        ix = 0;
                }
        }
        return -1;
}

int hash_insert(char *s, int i)
{
        int ix = hash_ix(s);
        while (hash_table[ix].name) {	// assumes no duplicate entries
                hash_collisions += 1;
                ix += 1;
                if (ix >= HASH_TABLE_SIZE) {
                        ix = 0;
                }
        }
        hash_table[ix].name = s;
        hash_table[ix].index = i;
        return ix;
}

// To decouple details of table display (e.g. column width, line folding for
// display screen width, et cetera) from acquiring the data and populating the
// tables, this semi-general table handling code is used.  There are various
// routines to set table attributes, assign and test some cell contents,
// initialize and actually display the table.

#define CELL_TYPE_NULL     0
#define CELL_TYPE_LONG     1
#define CELL_TYPE_DOUBLE   2
#define CELL_TYPE_STRING   3
#define CELL_TYPE_CHAR8    4
#define CELL_TYPE_REPCHAR  5

#define CELL_FLAG_FREEABLE (1 << 0)
#define CELL_FLAG_ROWSPAN  (1 << 1)
#define CELL_FLAG_COLSPAN  (1 << 2)

#define COL_JUSTIFY_LEFT       (1 << 0)
#define COL_JUSTIFY_RIGHT      (1 << 1)
#define COL_JUSTIFY_CENTER     3
#define COL_JUSTIFY_MASK       0x3
#define COL_FLAG_SEEN_DATA     (1 << 2)
#define COL_FLAG_NON_ZERO_DATA (1 << 3)
#define COL_FLAG_ALWAYS_SHOW   (1 << 4)

#define ROW_FLAG_SEEN_DATA     COL_FLAG_SEEN_DATA
#define ROW_FLAG_NON_ZERO_DATA COL_FLAG_NON_ZERO_DATA
#define ROW_FLAG_ALWAYS_SHOW   COL_FLAG_ALWAYS_SHOW

typedef struct cell {
        uint32_t type;
        uint32_t flags;
        union {
                char *s;
                double d;
                int64_t l;
                char c[8];
        };
} cell_t, *cell_p;

typedef struct vtab {
        int header_rows;
        int header_cols;
        int data_rows;
        int data_cols;
        cell_p cell;
        int *row_ix_map;
        uint8_t *row_flags;
        uint8_t *col_flags;
        uint8_t *col_width;
        uint8_t *col_decimal_places;
} vtab_t, *vtab_p;

#define ALL_TABLE_ROWS (table->header_rows + table->data_rows)
#define ALL_TABLE_COLS (table->header_cols + table->data_cols)
#define GET_CELL_PTR(row, col) (&table->cell[(row * ALL_TABLE_COLS) + col])

#define USUAL_GUTTER_WIDTH 1

void set_row_flag(vtab_p table, int row, int flag)
{
        table->row_flags[row] |= (uint8_t)flag;
}

void set_col_flag(vtab_p table, int col, int flag)
{
        table->col_flags[col] |= (uint8_t)flag;
}

void clear_row_flag(vtab_p table, int row, int flag)
{
        table->row_flags[row] &= (uint8_t)~flag;
}

void clear_col_flag(vtab_p table, int col, int flag)
{
        table->col_flags[col] &= (uint8_t)~flag;
}

int test_row_flag(vtab_p table, int row, int flag)
{
        return ((table->row_flags[row] & (uint8_t)flag) != 0);
}

int test_col_flag(vtab_p table, int col, int flag)
{
        return ((table->col_flags[col] & (uint8_t)flag) != 0);
}

void set_col_justification(vtab_p table, int col, int justify)
{
        table->col_flags[col] &= (uint8_t)~COL_JUSTIFY_MASK;
        table->col_flags[col] |= (uint8_t)(justify & COL_JUSTIFY_MASK);
}

void set_col_width(vtab_p table, int col, uint8_t width)
{
        if (width >= SMALL_BUF_SIZE) {
                width = SMALL_BUF_SIZE - 1;
        }
        table->col_width[col] = width;
}

void set_col_decimal_places(vtab_p table, int col, uint8_t places)
{
        table->col_decimal_places[col] = places;
}

void set_cell_flag(vtab_p table, int row, int col, int flag)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->flags |= (uint32_t)flag;
}

void clear_cell_flag(vtab_p table, int row, int col, int flag)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->flags &= (uint32_t)~flag;
}

int test_cell_flag(vtab_p table, int row, int col, int flag)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        return ((c_ptr->flags & (uint32_t)flag) != 0);
}

void string_assign(vtab_p table, int row, int col, char *s)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->type = CELL_TYPE_STRING;
        c_ptr->s = s;
}

void repchar_assign(vtab_p table, int row, int col, char c)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->type = CELL_TYPE_REPCHAR;
        c_ptr->c[0] = c;
}

void double_assign(vtab_p table, int row, int col, double d)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->type = CELL_TYPE_DOUBLE;
        c_ptr->d = d;
}

void long_assign(vtab_p table, int row, int col, int64_t l)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->type = CELL_TYPE_LONG;
        c_ptr->l = l;
}

void double_addto(vtab_p table, int row, int col, double d)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->type = CELL_TYPE_DOUBLE;
        c_ptr->d += d;
}

void long_addto(vtab_p table, int row, int col, int64_t l)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        c_ptr->type = CELL_TYPE_LONG;
        c_ptr->l += l;
}

void clear_assign(vtab_p table, int row, int col)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        memset(c_ptr, 0, sizeof(cell_t));
}

void zero_table_data(vtab_p table, int type)
{
        // Sets data area of table to zeros of specified type
        for (int row = table->header_rows; (row < ALL_TABLE_ROWS); row++) {
                for (int col = table->header_cols; (col < ALL_TABLE_COLS); col++) {
                        cell_p c_ptr = GET_CELL_PTR(row, col);
                        memset(c_ptr, 0, sizeof(cell_t));
                        c_ptr->type = type;
                }
        }
}

void sort_rows_descending_by_col(vtab_p table, int start_row, int stop_row, int col)
{
        // Rearrange row_ix_map[] indices so the rows will be in
        // descending order by the value in the specified column
        for (int ix = start_row; (ix <= stop_row); ix++) {
                int biggest_ix = ix;
                cell_p biggest_ix_c_ptr = GET_CELL_PTR(table->row_ix_map[ix], col);
                for (int iy = ix + 1; (iy <= stop_row); iy++) {
                        cell_p iy_c_ptr = GET_CELL_PTR(table->row_ix_map[iy], col);
                        if (biggest_ix_c_ptr->d < iy_c_ptr->d) {
                                biggest_ix_c_ptr = iy_c_ptr;
                                biggest_ix = iy;
                        }
                }
                if (biggest_ix != ix) {
                        int tmp = table->row_ix_map[ix];
                        table->row_ix_map[ix] = table->row_ix_map[biggest_ix];
                        table->row_ix_map[biggest_ix] = tmp;
                }
        }
}

void span(vtab_p table, int first_row, int first_col, int last_row, int last_col)
{
        // FIXME: implement row / col spannnig someday?
}

void init_table(vtab_p table, int header_rows, int header_cols, int data_rows, int data_cols)
{
        // init table sizes
        table->header_rows = header_rows;
        table->header_cols = header_cols;
        table->data_rows = data_rows;
        table->data_cols = data_cols;
        // allocate memory for all the cells
        int alloc_size = ALL_TABLE_ROWS * ALL_TABLE_COLS * sizeof(cell_t);
        table->cell = malloc(alloc_size);
        if (table->cell == NULL) {
                perror("malloc failed line: " STRINGIFY(__LINE__));
                exit(EXIT_FAILURE);
        }
        memset(table->cell, 0, alloc_size);
        // allocate memory for the row map vector
        alloc_size = ALL_TABLE_ROWS * sizeof(int);
        table->row_ix_map = malloc(alloc_size);
        if (table->row_ix_map == NULL) {
                perror("malloc failed line: " STRINGIFY(__LINE__));
                exit(EXIT_FAILURE);
        }
        for (int row = 0; (row < ALL_TABLE_ROWS); row++) {
                table->row_ix_map[row] = row;
        }
        // allocate memory for the row flags vector
        alloc_size = ALL_TABLE_ROWS * sizeof(uint8_t);
        table->row_flags = malloc(alloc_size);
        if (table->row_flags == NULL) {
                perror("malloc failed line: " STRINGIFY(__LINE__));
                exit(EXIT_FAILURE);
        }
        memset(table->row_flags, 0, alloc_size);
        // allocate memory for the column flags vector
        alloc_size = ALL_TABLE_COLS * sizeof(uint8_t);
        table->col_flags = malloc(alloc_size);
        if (table->col_flags == NULL) {
                perror("malloc failed line: " STRINGIFY(__LINE__));
                exit(EXIT_FAILURE);
        }
        memset(table->col_flags, 0, alloc_size);
        // allocate memory for the column width vector
        alloc_size = ALL_TABLE_COLS * sizeof(uint8_t);
        table->col_width = malloc(alloc_size);
        if (table->col_width == NULL) {
                perror("malloc failed line: " STRINGIFY(__LINE__));
                exit(EXIT_FAILURE);
        }
        memset(table->col_width, 0, alloc_size);
        // allocate memory for the column precision vector
        alloc_size = ALL_TABLE_COLS * sizeof(uint8_t);
        table->col_decimal_places = malloc(alloc_size);
        if (table->col_decimal_places == NULL) {
                perror("malloc failed line: " STRINGIFY(__LINE__));
                exit(EXIT_FAILURE);
        }
        memset(table->col_decimal_places, 0, alloc_size);
}

void free_cell(vtab_p table, int row, int col)
{
        cell_p c_ptr = GET_CELL_PTR(row, col);
        if ((c_ptr->type == CELL_TYPE_STRING)
                        && (c_ptr->flags & CELL_FLAG_FREEABLE)
                        && (c_ptr->s != NULL)) {
                free(c_ptr->s);
        }
        memset(c_ptr, 0, sizeof(cell_t));
}

void free_table(vtab_p table)
{
        if (table->cell != NULL) {
                for (int row = 0; (row < ALL_TABLE_ROWS); row++) {
                        for (int col = 0; (col < ALL_TABLE_COLS); col++) {
                                free_cell(table, row, col);
                        }
                }
                free(table->cell);
        }
        if (table->row_ix_map != NULL) {
                free(table->row_ix_map);
        }
        if (table->row_flags != NULL) {
                free(table->row_flags);
        }
        if (table->col_flags != NULL) {
                free(table->col_flags);
        }
        if (table->col_width != NULL) {
                free(table->col_width);
        }
        if (table->col_decimal_places != NULL) {
                free(table->col_decimal_places);
        }
}

char *fmt_cell_data(cell_p c_ptr, int max_width, int decimal_places)
{
        // Returns pointer to a static buffer, expecting caller to
        // immediately use or copy the contents before calling again.
        int rep_width = max_width - USUAL_GUTTER_WIDTH;
        static char buf[SMALL_BUF_SIZE];
        switch (c_ptr->type) {
        case CELL_TYPE_NULL:
                buf[0] = '\0';
                break;
        case CELL_TYPE_LONG:
                snprintf(buf, SMALL_BUF_SIZE, "%ld", c_ptr->l);
                break;
        case CELL_TYPE_DOUBLE:
                snprintf(buf, SMALL_BUF_SIZE, "%.*f", decimal_places, c_ptr->d);
                break;
        case CELL_TYPE_STRING:
                snprintf(buf, SMALL_BUF_SIZE, "%s", c_ptr->s);
                break;
        case CELL_TYPE_CHAR8:
                strncpy(buf, c_ptr->c, 8);
                buf[8] = '\0';
                break;
        case CELL_TYPE_REPCHAR:
                memset(buf, c_ptr->c[0], rep_width);
                buf[rep_width] = '\0';
                break;
        default:
                strcpy(buf, "Unknown");
                break;
        }
        buf[max_width] = '\0';
        return buf;
}

void auto_set_col_width(vtab_p table, int col, int min_width, int max_width)
{
        int width = min_width;
        for (int row = 0; (row < ALL_TABLE_ROWS); row++) {
                cell_p c_ptr = GET_CELL_PTR(row, col);
                if (c_ptr->type == CELL_TYPE_REPCHAR) {
                        continue;
                }
                char *p = fmt_cell_data(c_ptr, max_width, (int)(table->col_decimal_places[col]));
                int l = strlen(p);
                if (width < l) {
                        width = l;
                }
        }
        width += USUAL_GUTTER_WIDTH;
        if (width > max_width) {
                width = max_width;
        }
        table->col_width[col] = (uint8_t)width;
}

void display_justified_cell(cell_p c_ptr, int row_flags, int col_flags, int width, int decimal_places)
{
        char *p = fmt_cell_data(c_ptr, width, decimal_places);
        int l = strlen(p);
        char buf[SMALL_BUF_SIZE];
        switch (col_flags & COL_JUSTIFY_MASK) {
        case COL_JUSTIFY_LEFT:
                memcpy(buf, p, l);
                if (l < width) {
                        memset(&buf[l], ' ', width - l);
                }
                break;
        case COL_JUSTIFY_RIGHT:
                if (l < width) {
                        memset(buf, ' ', width - l);
                }
                memcpy(&buf[width - l], p, l);
                break;
        case COL_JUSTIFY_CENTER:
        default:
                memset(buf, ' ', width);
                memcpy(&buf[(width - l + 1) / 2], p, l);
                break;
        }
        buf[width] = '\0';
        printf("%s", buf);
}

void display_table(vtab_p table,
                   int screen_width,
                   int show_unseen_rows,
                   int show_unseen_cols,
                   int show_zero_rows,
                   int show_zero_cols)
{
        // Set row and column flags according to whether data in rows and cols
        // has been assigned, and is currently non-zero.
        int some_seen_data = 0;
        int some_non_zero_data = 0;
        for (int row = table->header_rows; (row < ALL_TABLE_ROWS); row++) {
                for (int col = table->header_cols; (col < ALL_TABLE_COLS); col++) {
                        cell_p c_ptr = GET_CELL_PTR(row, col);
                        // Currently, "seen data" includes not only numeric data, but also
                        // any strings, etc -- anything non-NULL (other than rephcars).
                        if ((c_ptr->type != CELL_TYPE_NULL) && (c_ptr->type != CELL_TYPE_REPCHAR)) {
                                some_seen_data = 1;
                                set_row_flag(table, row, ROW_FLAG_SEEN_DATA);
                                set_col_flag(table, col, COL_FLAG_SEEN_DATA);
                                // Currently, "non-zero data" includes not only numeric data,
                                // but also any strings, etc -- anything non-zero (other than
                                // repchars, which are already excluded above).  So, note a
                                // valid non-NULL pointer to an empty string would still be
                                // counted as non-zero data.
                                if (c_ptr->l != (int64_t)0) {
                                        some_non_zero_data = 1;
                                        set_row_flag(table, row, ROW_FLAG_NON_ZERO_DATA);
                                        set_col_flag(table, col, COL_FLAG_NON_ZERO_DATA);
                                }
                        }
                }
        }
        if (!some_seen_data) {
                printf("Table has no data.\n");
                return;
        }
        if (!some_non_zero_data && !show_zero_rows && !show_zero_cols) {
                printf("Table has no non-zero data.\n");
                return;
        }
        // Start with first data column and try to display table,
        // folding lines as necessary per screen_width
        int col = -1;
        int data_col = table->header_cols;
        while (data_col < ALL_TABLE_COLS) {
                // Skip data columns until we have one to display
                if ((!test_col_flag(table, data_col, COL_FLAG_ALWAYS_SHOW)) &&
                                (((!show_unseen_cols) && (!test_col_flag(table, data_col, COL_FLAG_SEEN_DATA))) ||
                                 ((!show_zero_cols)   && (!test_col_flag(table, data_col, COL_FLAG_NON_ZERO_DATA))))) {
                        data_col += 1;
                        continue;
                }
                // Display blank line between table sections
                if (col > 0) {
                        printf("\n");
                }
                // For each row, display as many columns as possible
                for (int row_ix = 0; (row_ix < ALL_TABLE_ROWS); row_ix++) {
                        int row = table->row_ix_map[row_ix];
                        // If past the header rows, conditionally skip rows
                        if ((row >= table->header_rows) && (!test_row_flag(table, row, ROW_FLAG_ALWAYS_SHOW))) {
                                // Optionally skip row if no data seen or if all zeros
                                if (((!show_unseen_rows) && (!test_row_flag(table, row, ROW_FLAG_SEEN_DATA))) ||
                                                ((!show_zero_rows)   && (!test_row_flag(table, row, ROW_FLAG_NON_ZERO_DATA)))) {
                                        continue;
                                }
                        }
                        // Begin a new row...
                        int cur_line_width = 0;
                        // All lines start with the left header columns
                        for (col = 0; (col < table->header_cols); col++) {
                                display_justified_cell(GET_CELL_PTR(row, col),
                                                       (int)(table->row_flags[row]),
                                                       (int)(table->col_flags[col]),
                                                       (int)(table->col_width[col]),
                                                       (int)(table->col_decimal_places[col]));
                                cur_line_width += (int)(table->col_width[col]);
                        }
                        // Reset column index to starting data column for each new row
                        col = data_col;
                        // Try to display as many data columns as possible in every section
                        for (;;) {
                                // See if we should print this column
                                if (test_col_flag(table, col, COL_FLAG_ALWAYS_SHOW) ||
                                                (((show_unseen_cols) || (test_col_flag(table, col, COL_FLAG_SEEN_DATA))) &&
                                                 ((show_zero_cols)   || (test_col_flag(table, col, COL_FLAG_NON_ZERO_DATA))))) {
                                        display_justified_cell(GET_CELL_PTR(row, col),
                                                               (int)(table->row_flags[row]),
                                                               (int)(table->col_flags[col]),
                                                               (int)(table->col_width[col]),
                                                               (int)(table->col_decimal_places[col]));
                                        cur_line_width += (int)(table->col_width[col]);
                                }
                                col += 1;
                                // End the line if no more columns or next column would exceed screen width
                                if ((col >= ALL_TABLE_COLS) ||
                                                ((cur_line_width + (int)(table->col_width[col])) > screen_width)) {
                                        break;
                                }
                        }
                        printf("\n");
                }
                // Remember next starting data column for next section
                data_col = col;
        }
}

int verbose = 0;
int num_pids = 0;
int num_nodes = 0;
int screen_width = 0;
int show_zero_data = 1;
int compress_display = 0;
int sort_table = 0;
int sort_table_node = -1;
int compatibility_mode = 0;
int pid_array_max_pids = 0;
int *pid_array = NULL;
char *prog_name = NULL;
double page_size_in_bytes = 0;
double huge_page_size_in_bytes = 0;

void display_version_and_exit(void)
{
        char *version_string = "20130723";
        printf("%s version: %s: %s\n", prog_name, version_string, __DATE__);
        exit(EXIT_SUCCESS);
}

void display_usage_and_exit(void)
{
        fprintf(stderr, "Usage: %s [-c] [-m] [-n] [-p <PID>|<pattern>] [-s[<node>]] [-v] [-V] [-z] [ <PID>|<pattern>... ]\n", prog_name);
        fprintf(stderr, "-c to minimize column widths\n");
        fprintf(stderr, "-m to show meminfo-like system-wide memory usage\n");
        fprintf(stderr, "-n to show the numastat statistics info\n");
        fprintf(stderr, "-p <PID>|<pattern> to show process info\n");
        fprintf(stderr, "-s[<node>] to sort data by total column or <node>\n");
        fprintf(stderr, "-v to make some reports more verbose\n");
        fprintf(stderr, "-V to show the %s code version\n", prog_name);
        fprintf(stderr, "-z to skip rows and columns of zeros\n");
        exit(EXIT_FAILURE);
}

int get_screen_width(void)
{
        int width = 80;
        char *p = getenv("NUMASTAT_WIDTH");
        if (p != NULL) {
                width = atoi(p);
                if ((width < 1) || (width > 10000000)) {
                        width = 80;
                }
        } else if (isatty(fileno(stdout))) {
                FILE *fs = popen("resize 2>/dev/null", "r");
                if (fs != NULL) {
                        char buf[72];
                        char *columns;
                        columns = fgets(buf, sizeof(columns), fs);
                        pclose(fs);
                        if (columns && strncmp(columns, "COLUMNS=", 8) == 0) {
                                width = atoi(&columns[8]);
                                if ((width < 1) || (width > 10000000)) {
                                        width = 80;
                                }
                        }
                }
        } else {
                // Not a tty, so allow a really long line
                width = 10000000;
        }
        if (width < 32) {
                width = 32;
        }
        return width;
}

char *command_name_for_pid(int pid)
{
        // Get the PID command name field from /proc/PID/status file.  Return
        // pointer to a static buffer, expecting caller to immediately copy result.
        static char buf[SMALL_BUF_SIZE];
        char fname[64];
        snprintf(fname, sizeof(fname), "/proc/%d/status", pid);
        FILE *fs = fopen(fname, "r");
        if (!fs) {
                return NULL;
        } else {
                while (fgets(buf, SMALL_BUF_SIZE, fs)) {
                        if (strstr(buf, "Name:") == buf) {
                                char *p = &buf[5];
                                while (isspace(*p)) {
                                        p++;
                                }
                                if (p[strlen(p) - 1] == '\n') {
                                        p[strlen(p) - 1] = '\0';
                                }
                                fclose(fs);
                                return p;
                        }
                }
                fclose(fs);
        }
        return NULL;
}

void show_info_from_system_file(char *file, meminfo_p meminfo, int meminfo_rows, int tok_offset)
{
        // Setup and init table
        vtab_t table;
        int header_rows = 2 - compatibility_mode;
        int header_cols = 1;
        // Add an extra data column for a total column
        init_table(&table, header_rows, header_cols, meminfo_rows, num_nodes + 1);
        int total_col_ix = header_cols + num_nodes;
        // Insert token mapping in hash table and assign left header column label for each row in table
        init_hash_table();
        for (int row = 0; (row < meminfo_rows); row++) {
                hash_insert(meminfo[row].token, meminfo[row].index);
                if (compatibility_mode) {
                        string_assign(&table, (header_rows + row), 0, meminfo[row].token);
                } else {
                        string_assign(&table, (header_rows + row), 0, meminfo[row].label);
                }
        }
        // printf("There are %d table hash collisions.\n", hash_collisions);
        // Set left header column width and left justify it
        set_col_width(&table, 0, 16);
        set_col_justification(&table, 0, COL_JUSTIFY_LEFT);
        // Open /sys/devices/system/node/node?/<file> for each node and store data
        // in table.  If not compatibility_mode, do approximately first third of
        // this loop also for (node_ix == num_nodes) to get "Total" column header.
        for (int node_ix = 0; (node_ix < (num_nodes + (1 - compatibility_mode))); node_ix++) {
                int col = header_cols + node_ix;
                // Assign header row label and horizontal line for this column...
                string_assign(&table, 0, col, node_header[node_ix]);
                if (!compatibility_mode) {
                        repchar_assign(&table, 1, col, '-');
                        int decimal_places = 2;
                        if (compress_display) {
                                decimal_places = 0;
                        }
                        set_col_decimal_places(&table, col, decimal_places);
                }
                // Set column width and right justify data
                set_col_width(&table, col, 16);
                set_col_justification(&table, col, COL_JUSTIFY_RIGHT);
                if (node_ix == num_nodes) {
                        break;
                }
                // Open /sys/.../node<N>/numstast file for this node...
                char buf[SMALL_BUF_SIZE];
                char fname[64];
                snprintf(fname, sizeof(fname), "/sys/devices/system/node/node%d/%s", node_ix_map[node_ix], file);
                FILE *fs = fopen(fname, "r");
                if (!fs) {
                        sprintf(buf, "cannot open %s", fname);
                        perror(buf);
                        exit(EXIT_FAILURE);
                }
                // Get table values for this node...
                while (fgets(buf, SMALL_BUF_SIZE, fs)) {
                        char *tok[64];
                        int tokens = 0;
                        const char *delimiters = " \t\r\n:";
                        char *p = strtok(buf, delimiters);
                        if (p == NULL) {
                                continue;	// Skip blank lines;
                        }
                        while (p) {
                                tok[tokens++] = p;
                                p = strtok(NULL, delimiters);
                        }
                        // example line from numastat file: "numa_miss 16463"
                        // example line from meminfo  file: "Node 3 Inactive:  210680 kB"
                        int index = hash_lookup(tok[0 + tok_offset]);
                        if (index < 0) {
                                printf("Token %s not in hash table.\n", tok[0 + tok_offset]);
                        } else {
                                double value = (double)atol(tok[1 + tok_offset]);
                                if (!compatibility_mode) {
                                        double multiplier = 1.0;
                                        if (tokens < 4) {
                                                multiplier = page_size_in_bytes;
                                        } else if (!strncmp("HugePages", tok[2], 9)) {
                                                multiplier = huge_page_size_in_bytes;
                                        } else if (!strncmp("kB", tok[4], 2)) {
                                                multiplier = KILOBYTE;
                                        }
                                        value *= multiplier;
                                        value /= (double)MEGABYTE;
                                }
                                double_assign(&table, header_rows + index, col, value);
                                double_addto(&table, header_rows + index, total_col_ix, value);
                        }
                }
                fclose(fs);
        }
        // Crompress display column widths, if requested
        if (compress_display) {
                for (int col = 0; (col < header_cols + num_nodes + 1); col++) {
                        auto_set_col_width(&table, col, 4, 16);
                }
        }
        // Optionally sort the table data
        if (sort_table) {
                int sort_col;
                if ((sort_table_node < 0) || (sort_table_node >= num_nodes)) {
                        sort_col = total_col_ix;
                } else {
                        sort_col = header_cols + node_ix_map[sort_table_node];
                }
                sort_rows_descending_by_col(&table, header_rows, header_rows + meminfo_rows - 1, sort_col);
        }
        // Actually display the table now, doing line-folding as necessary
        display_table(&table, screen_width, 0, 0, show_zero_data, show_zero_data);
        free_table(&table);
}

void show_numastat_info(void)
{
        if (!compatibility_mode) {
                printf("\nPer-node numastat info (in MBs):\n");
        }
        show_info_from_system_file("numastat", numastat_meminfo, NUMASTAT_MEMINFO_ROWS, 0);
}

void show_system_info(void)
{
        printf("\nPer-node system memory usage (in MBs):\n");
        show_info_from_system_file("meminfo", system_meminfo, SYSTEM_MEMINFO_ROWS, 2);
}

void show_process_info(void)
{
        vtab_t table;
        int header_rows = 2;
        int header_cols = 1;
        int data_rows;
        int show_sub_categories = (verbose || (num_pids == 1));
        if (show_sub_categories) {
                data_rows = PROCESS_MEMINFO_ROWS;
        } else {
                data_rows = num_pids;
        }
        // Add two extra rows for a horizontal rule followed by a total row
        // Add one extra data column for a total column
        init_table(&table, header_rows, header_cols, data_rows + 2, num_nodes + 1);
        int total_col_ix = header_cols + num_nodes;
        int total_row_ix = header_rows + data_rows + 1;
        string_assign(&table, total_row_ix, 0, "Total");
        if (show_sub_categories) {
                // Assign left header column label for each row in table
                for (int row = 0; (row < PROCESS_MEMINFO_ROWS); row++) {
                        string_assign(&table, (header_rows + row), 0, process_meminfo[row].label);
                }
        } else {
                string_assign(&table, 0, 0, "PID");
                repchar_assign(&table, 1, 0, '-');
                printf("\nPer-node process memory usage (in MBs)\n");
        }
        // Set left header column width and left justify it
        set_col_width(&table, 0, 16);
        set_col_justification(&table, 0, COL_JUSTIFY_LEFT);
        // Set up "Node <N>" column headers over data columns, plus "Total" column
        for (int node_ix = 0; (node_ix <= num_nodes); node_ix++) {
                int col = header_cols + node_ix;
                // Assign header row label and horizontal line for this column...
                string_assign(&table, 0, col, node_header[node_ix]);
                repchar_assign(&table, 1, col, '-');
                // Set column width, decimal places, and right justify data
                set_col_width(&table, col, 16);
                int decimal_places = 2;
                if (compress_display) {
                        decimal_places = 0;
                }
                set_col_decimal_places(&table, col, decimal_places);
                set_col_justification(&table, col, COL_JUSTIFY_RIGHT);
        }
        // Initialize data in table to all zeros
        zero_table_data(&table, CELL_TYPE_DOUBLE);
        // If (show_sub_categories), show individual process tables for each PID,
        // Otherwise show one big table of process total lines from all the PIDs.
        for (int pid_ix = 0; (pid_ix < num_pids); pid_ix++) {
                int pid = pid_array[pid_ix];
                if (show_sub_categories) {
                        printf("\nPer-node process memory usage (in MBs) for PID %d (%s)\n", pid, command_name_for_pid(pid));
                        if (pid_ix > 0) {
                                // Re-initialize show_sub_categories table, because we re-use it for each PID.
                                zero_table_data(&table, CELL_TYPE_DOUBLE);
                        }
                } else {
                        // Put this row's "PID (cmd)" label in left header column for this PID total row
                        char tmp_buf[64];
                        snprintf(tmp_buf, sizeof(tmp_buf), "%d (%s)", pid, command_name_for_pid(pid));
                        char *p = strdup(tmp_buf);
                        if (p == NULL) {
                                perror("malloc failed line: " STRINGIFY(__LINE__));
                                exit(EXIT_FAILURE);
                        }
                        string_assign(&table, header_rows + pid_ix, 0, p);
                        set_cell_flag(&table, header_rows + pid_ix, 0, CELL_FLAG_FREEABLE);
                }
                // Open numa_map for this PID to get per-node data
                char fname[64];
                snprintf(fname, sizeof(fname), "/proc/%d/numa_maps", pid);
                char buf[BUF_SIZE];
                FILE *fs = fopen(fname, "r");
                if (!fs) {
                        sprintf(buf, "Can't read /proc/%d/numa_maps", pid);
                        perror(buf);
                        continue;
                }
                // Add up sub-category memory used from each node.  Must go line by line
                // through the numa_map figuring out which category memory, node, and the
                // amount.
                while (fgets(buf, BUF_SIZE, fs)) {
                        int category = PROCESS_PRIVATE_INDEX;	// init category to the catch-all...
                        const char *delimiters = " \t\r\n";
                        char *p = strtok(buf, delimiters);
                        while (p) {
                                // If the memory category for this line is still the catch-all
                                // (i.e.  private), then see if the current token is a special
                                // keyword for a specific memory sub-category.
                                if (category == PROCESS_PRIVATE_INDEX) {
                                        for (int ix = 0; (ix < PROCESS_PRIVATE_INDEX); ix++) {
                                                if (!strncmp(p, process_meminfo[ix].token, strlen(process_meminfo[ix].token))) {
                                                        category = ix;
                                                        break;
                                                }
                                        }
                                }
                                // If the current token is a per-node pages quantity, parse the
                                // node number and accumulate the number of pages in the specific
                                // category (and also add to the total).
                                if (p[0] == 'N') {
                                        int node_num = (int)strtol(&p[1], &p, 10);
                                        if (p[0] != '=') {
                                                perror("node value parse error");
                                                exit(EXIT_FAILURE);
                                        }
                                        double value = (double)strtol(&p[1], &p, 10);
                                        double multiplier = page_size_in_bytes;
                                        if (category == PROCESS_HUGE_INDEX) {
                                                multiplier = huge_page_size_in_bytes;
                                        }
                                        value *= multiplier;
                                        value /= (double)MEGABYTE;
                                        // Add value to data cell, total_col, and total_row
                                        int tmp_row;
                                        if (show_sub_categories) {
                                                tmp_row = header_rows + category;
                                        } else {
                                                tmp_row = header_rows + pid_ix;
                                        }
                                        // Don't assume nodes are sequential or contiguous.
                                        // Need to find correct tmp_col from node_ix_map
                                        int i = 0;
                                        while(node_ix_map[i++] != node_num)
                                                ;
                                        int tmp_col = header_cols + i - 1;
                                        double_addto(&table, tmp_row, tmp_col, value);
                                        double_addto(&table, tmp_row, total_col_ix, value);
                                        double_addto(&table, total_row_ix, tmp_col, value);
                                        double_addto(&table, total_row_ix, total_col_ix, value);
                                }
                                // Get next token on the line
                                p = strtok(NULL, delimiters);
                        }
                }
                // Currently, a non-root user can open some numa_map files successfully
                // without error, but can't actually read the contents -- despite the
                // 444 file permissions.  So, use ferror() to check here to see if we
                // actually got a read error, and if so, alert the user so they know
                // not to trust the zero in the table.
                if (ferror(fs)) {
                        sprintf(buf, "Can't read /proc/%d/numa_maps", pid);
                        perror(buf);
                        exit(EXIT_FAILURE);
                }
                fclose(fs);
                // If showing individual tables, or we just added the last total line,
                // prepare the table for display and display it...
                if ((show_sub_categories) || (pid_ix + 1 == num_pids)) {
                        // Crompress display column widths, if requested
                        if (compress_display) {
                                for (int col = 0; (col < header_cols + num_nodes + 1); col++) {
                                        auto_set_col_width(&table, col, 4, 16);
                                }
                        } else {
                                // Since not compressing the display, allow the left header
                                // column to be wider.  Otherwise, sometimes process command
                                // name instance numbers can be truncated in an annoying way.
                                auto_set_col_width(&table, 0, 16, 24);
                        }
                        // Put dashes above Total line...
                        set_row_flag(&table, total_row_ix - 1, COL_FLAG_ALWAYS_SHOW);
                        for (int col = 0; (col < header_cols + num_nodes + 1); col++) {
                                repchar_assign(&table, total_row_ix - 1, col, '-');
                        }
                        // Optionally sort the table data
                        if (sort_table) {
                                int sort_col;
                                if ((sort_table_node < 0) || (sort_table_node >= num_nodes)) {
                                        sort_col = total_col_ix;
                                } else {
                                        sort_col = header_cols + node_ix_map[sort_table_node];
                                }
                                sort_rows_descending_by_col(&table, header_rows, header_rows + data_rows - 1, sort_col);
                        }
                        // Actually show the table
                        display_table(&table, screen_width, 0, 0, show_zero_data, show_zero_data);
                }
        }			// END OF FOR_EACH-PID loop
        free_table(&table);
}				// show_process_info()

int node_and_digits(const struct dirent *dptr)
{
        char *p = (char *)(dptr->d_name);
        if (*p++ != 'n') return 0;
        if (*p++ != 'o') return 0;
        if (*p++ != 'd') return 0;
        if (*p++ != 'e') return 0;
        do {
                if (!isdigit(*p++)) return 0;
        } while (*p != '\0');
        return 1;
}

void init_node_ix_map_and_header(int compatibility_mode)
{
        // Count directory names of the form: /sys/devices/system/node/node<N>
        struct dirent **namelist;
        num_nodes = scandir("/sys/devices/system/node", &namelist, node_and_digits, NULL);
        if (num_nodes < 1) {
                if (compatibility_mode) {
                        perror("sysfs not mounted or system not NUMA aware");
                } else {
                        perror("Couldn't open /sys/devices/system/node");
                }
                exit(EXIT_FAILURE);
        } else {
                node_ix_map = malloc(num_nodes * sizeof(int));
                if (node_ix_map == NULL) {
                        perror("malloc failed line: " STRINGIFY(__LINE__));
                        exit(EXIT_FAILURE);
                }
                // For each "node<N>" filename present, save <N> in node_ix_map
                for (int ix = 0; (ix < num_nodes); ix++) {
                        node_ix_map[ix] = atoi(&namelist[ix]->d_name[4]);
                        free(namelist[ix]);
                }
                free(namelist);
                // Now, sort the node map in increasing order. Use a simplistic sort
                // since we expect a relatively short (and maybe pre-ordered) list.
                for (int ix = 0; (ix < num_nodes); ix++) {
                        int smallest_ix = ix;
                        for (int iy = ix + 1; (iy < num_nodes); iy++) {
                                if (node_ix_map[smallest_ix] > node_ix_map[iy]) {
                                        smallest_ix = iy;
                                }
                        }
                        if (smallest_ix != ix) {
                                int tmp = node_ix_map[ix];
                                node_ix_map[ix] = node_ix_map[smallest_ix];
                                node_ix_map[smallest_ix] = tmp;
                        }
                }
                // Construct vector of "Node <N>" and "Total" column headers. Allocate
                // one for each NUMA node, plus one on the end for the "Total" column
                node_header = malloc((num_nodes + 1) * sizeof(char *));
                if (node_header == NULL) {
                        perror("malloc failed line: " STRINGIFY(__LINE__));
                        exit(EXIT_FAILURE);
                }
                for (int node_ix = 0; (node_ix <= num_nodes); node_ix++) {
                        char node_label[64];
                        if (node_ix == num_nodes) {
                                strcpy(node_label, "Total");
                        } else if (compatibility_mode) {
                                snprintf(node_label, sizeof(node_label), "node%d", node_ix_map[node_ix]);
                        } else {
                                snprintf(node_label, sizeof(node_label), "Node %d", node_ix_map[node_ix]);
                        }
                        char *s = strdup(node_label);
                        if (s == NULL) {
                                perror("malloc failed line: " STRINGIFY(__LINE__));
                                exit(EXIT_FAILURE);
                        }
                        node_header[node_ix] = s;
                }
        }
}

void free_node_ix_map_and_header(void)
{
        if (node_ix_map != NULL) {
                free(node_ix_map);
                node_ix_map = NULL;
        }
        if (node_header != NULL) {
                for (int ix = 0; (ix <= num_nodes); ix++) {
                        free(node_header[ix]);
                }
                free(node_header);
                node_header = NULL;
        }
}

double get_huge_page_size_in_bytes(void)
{
        double huge_page_size = 0;;
        FILE *fs = fopen("/proc/meminfo", "r");
        if (!fs) {
                perror("Can't open /proc/meminfo");
                exit(EXIT_FAILURE);
        }
        char buf[SMALL_BUF_SIZE];
        while (fgets(buf, SMALL_BUF_SIZE, fs)) {
                if (!strncmp("Hugepagesize", buf, 12)) {
                        char *p = &buf[12];
                        while ((!isdigit(*p)) && (p < buf + SMALL_BUF_SIZE)) {
                                p++;
                        }
                        huge_page_size = strtod(p, NULL);
                        break;
                }
        }
        fclose(fs);
        return huge_page_size * KILOBYTE;
}

int all_digits(char *p)
{
        if (p == NULL) {
                return 0;
        }
        while (*p != '\0') {
                if (!isdigit(*p++)) return 0;
        }
        return 1;
}

int starts_with_digit(const struct dirent *dptr)
{
        return (isdigit(dptr->d_name[0]));
}

void add_pid_to_list(int pid)
{
        if (num_pids < pid_array_max_pids) {
                pid_array[num_pids++] = pid;
        } else {
                if (pid_array_max_pids == 0) {
                        pid_array_max_pids = 32;
                }
                int *tmp_int_ptr = realloc(pid_array, 2 * pid_array_max_pids * sizeof(int));
                if (tmp_int_ptr == NULL) {
                        char buf[SMALL_BUF_SIZE];
                        sprintf(buf, "Too many PIDs, skipping %d", pid);
                        perror(buf);
                } else {
                        pid_array = tmp_int_ptr;
                        pid_array_max_pids *= 2;
                        pid_array[num_pids++] = pid;
                }
        }
}

int ascending(const void *p1, const void *p2)
{
        return *(int *)p1 - *(int *) p2;
}

void sort_pids_and_remove_duplicates(void)
{
        if (num_pids > 1) {
                qsort(pid_array, num_pids, sizeof(int), ascending);
                int ix1 = 0;
                for (int ix2 = 1; (ix2 < num_pids); ix2++) {
                        if (pid_array[ix2] == pid_array[ix1]) {
                                continue;
                        }
                        ix1 += 1;
                        if (ix2 > ix1) {
                                pid_array[ix1] = pid_array[ix2];
                        }
                }
                num_pids = ix1 + 1;
        }
}

void add_pids_from_pattern_search(char *pattern)
{
        // Search all /proc/<PID>/cmdline files and /proc/<PID>/status:Name fields
        // for matching patterns.  Show the memory details for matching PIDs.
        int num_matches_found = 0;
        struct dirent **namelist;
        int files = scandir("/proc", &namelist, starts_with_digit, NULL);
        if (files < 0) {
                perror("Couldn't open /proc");
        }
        for (int ix = 0; (ix < files); ix++) {
                char buf[BUF_SIZE];
                // First get Name field from status file
                int pid = atoi(namelist[ix]->d_name);
                char *p = command_name_for_pid(pid);
                if (p) {
                        strcpy(buf, p);
                } else {
                        buf[0] = '\0';
                }
                // Next copy cmdline file contents onto end of buffer.  Do it a
                // character at a time to convert nulls to spaces.
                char fname[272];
                snprintf(fname, sizeof(fname), "/proc/%s/cmdline", namelist[ix]->d_name);
                FILE *fs = fopen(fname, "r");
                if (fs) {
                        p = buf;
                        while (*p != '\0') {
                                p++;
                        }
                        *p++ = ' ';
                        int c;
                        while (((c = fgetc(fs)) != EOF) && (p < buf + BUF_SIZE - 1)) {
                                if (c == '\0') {
                                        c = ' ';
                                }
                                *p++ = c;
                        }
                        *p++ = '\0';
                        fclose(fs);
                }
                if (strstr(buf, pattern)) {
                        if (pid != getpid()) {
                                add_pid_to_list(pid);
                                num_matches_found += 1;
                        }
                }
                free(namelist[ix]);
        }
        free(namelist);
        if (num_matches_found == 0) {
                printf("Found no processes containing pattern: \"%s\"\n", pattern);
        }
}

int main(int argc, char **argv)
{
        prog_name = argv[0];
        int show_the_system_info = 0;
        int show_the_numastat_info = 0;
        static struct option long_options[] = {
                {"help", 0, 0, '?'},
                {0, 0, 0, 0}
        };
        int long_option_index = 0;
        int opt;
        while ((opt = getopt_long(argc, argv, "cmnp:s::vVz?", long_options, &long_option_index)) != -1) {
                switch (opt) {
                case 0:
                        printf("Unexpected long option %s", long_options[long_option_index].name);
                        if (optarg) {
                                printf(" with arg %s", optarg);
                        }
                        printf("\n");
                        display_usage_and_exit();
                        break;
                case 'c':
                        compress_display = 1;
                        break;
                case 'm':
                        show_the_system_info = 1;
                        break;
                case 'n':
                        show_the_numastat_info = 1;
                        break;
                case 'p':
                        if ((optarg) && (all_digits(optarg))) {
                                add_pid_to_list(atoi(optarg));
                        } else {
                                add_pids_from_pattern_search(optarg);
                        }
                        break;
                case 's':
                        sort_table = 1;
                        if ((optarg) && (all_digits(optarg))) {
                                sort_table_node = atoi(optarg);
                        }
                        break;
                case 'v':
                        verbose = 1;
                        break;
                case 'V':
                        display_version_and_exit();
                        break;
                case 'z':
                        show_zero_data = 0;
                        break;
                default:
                case '?':
                        display_usage_and_exit();
                        break;
                }
        }
        // Figure out the display width, which is used to format the tables
        // and limit the output columns per row
        screen_width = get_screen_width();
        // Any remaining arguments are assumed to be additional process specifiers
        while (optind < argc) {
                if (all_digits(argv[optind])) {
                        add_pid_to_list(atoi(argv[optind]));
                } else {
                        add_pids_from_pattern_search(argv[optind]);
                }
                optind += 1;
        }
        // If there are no program options or arguments, be extremely compatible
        // with the old numastat perl script (which is included at the end of this
        // file for reference)
        compatibility_mode = (argc == 1);
        init_node_ix_map_and_header(compatibility_mode);	// enumarate the NUMA nodes
        if (compatibility_mode) {
                show_numastat_info();
                free_node_ix_map_and_header();
                exit(EXIT_SUCCESS);
        }
        // Figure out page sizes
        page_size_in_bytes = (double)sysconf(_SC_PAGESIZE);
        huge_page_size_in_bytes = get_huge_page_size_in_bytes();
        // Display the info for the process specifiers
        if (num_pids > 0) {
                sort_pids_and_remove_duplicates();
                show_process_info();
        }
        if (pid_array != NULL) {
                free(pid_array);
        }
        // Display the system-wide memory usage info
        if (show_the_system_info) {
                show_system_info();
        }
        // Display the numastat statistics info
        if ((show_the_numastat_info) || ((num_pids == 0) && (!show_the_system_info))) {
                show_numastat_info();
        }
        free_node_ix_map_and_header();
        exit(EXIT_SUCCESS);
}

#if 0
/*

#!/usr/bin/perl
# Print numa statistics for all nodes
# Copyright (C) 2003,2004 Andi Kleen, SuSE Labs.
#
# numastat is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public
# License as published by the Free Software Foundation; version
# 2.
#
# numastat is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.

# You should find a copy of v2 of the GNU General Public License somewhere
# on your Linux system; if not, write to the Free Software Foundation,
# Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
#
# Example: NUMASTAT_WIDTH=80 watch -n1 numastat
#

# output width
$WIDTH=80;
if (defined($ENV{'NUMASTAT_WIDTH'})) {
	$WIDTH=$ENV{'NUMASTAT_WIDTH'};
} else {
	use POSIX;
	if (POSIX::isatty(fileno(STDOUT))) {
		if (open(R, "resize |")) {
			while (<R>) {
				$WIDTH=$1 if /COLUMNS=(\d+)/;
			}
			close R;
		}
	} else {
		# don't split it up for easier parsing
		$WIDTH=10000000;
	}
}
$WIDTH = 32 if $WIDTH < 32;

if (! -d "/sys/devices/system/node" ) {
	print STDERR "sysfs not mounted or system not NUMA aware\n";
	exit 1;
}

%stat = ();
$title = "";
$mode = 0;
opendir(NODES, "/sys/devices/system/node") || exit 1;
foreach $nd (readdir(NODES)) {
	next unless $nd =~ /node(\d+)/;
	# On newer kernels, readdir may enumerate the 'node(\d+) subdirs
	# in opposite order from older kernels--e.g., node{0,1,2,...}
	# as opposed to node{N,N-1,N-2,...}.  Accomodate this by
	# switching to new mode so that the stats get emitted in
	# the same order.
        #print "readdir(NODES) returns $nd\n";
	if (!$title && $nd =~ /node0/) {
		$mode = 1;
	}
	open(STAT, "/sys/devices/system/node/$nd/numastat") ||
			die "cannot open $nd: $!\n";
	if (! $mode) {
		$title = sprintf("%16s",$nd) . $title;
	} else {
		$title = $title . sprintf("%16s",$nd);
	}
	@fields = ();
	while (<STAT>) {
		($name, $val) = split;
		if (! $mode) {
			$stat{$name} = sprintf("%16u", $val) . $stat{$name};
		} else {
			$stat{$name} = $stat{$name} . sprintf("%16u", $val);
		}
		push(@fields, $name);
	}
	close STAT;
}
closedir NODES;

$numfields = int(($WIDTH - 16) / 16);
$l = 16 * $numfields;
for ($i = 0; $i < length($title); $i += $l) {
	print "\n" if $i > 0;
	printf "%16s%s\n","",substr($title,$i,$l);
	foreach (@fields) {
		printf "%-16s%s\n",$_,substr($stat{$_},$i,$l);
	}
}

*/
#endif
