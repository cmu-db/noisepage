class TextTable(object):
    """
    Print out data as text, in a formatted table. This is used primarily for
    local testing and for the output in Jenkins
    """

    def __init__(self):
        self.rows = []
        # Columns to print, each item is a dictinary
        self.columns = []
        return

    def add_row(self, item):
        """ item - dictionary or object with attributes
        """
        self.rows.append(item)
        return

    def add_column(self, column, heading=None, col_format=None,
                   right_justify=False):
        """ Add single column (by name), to be printed
            column : dictionary key of column
            heading: heading to print for column. If not specified,
                     uses the column key
            format: optional format for column
            right_justify: overrides default format justification
        """
        col_dict = {}
        col_dict['name'] = column
        if not col_format is None:
            col_dict['format'] = " " + col_format + " "
        if not heading is None:
            col_dict['heading'] = heading
        if right_justify:
            col_dict['right_justify'] = True
        self.columns.append(col_dict)
        return

    def _width(self, row, col):
        """ Return width of field (dictionary) """
        return len(self._col_str_dict(row, col))

    def _col_str_dict(self, row, col):
        """ Return printable field (dictionary) """
        field = col['name']
        if not field in row:
            return ""
        if 'format' in col:
            return col['format'].format(row[field])
        return " {} ".format(row[field])

    def _set_col_widths(self):
        """ Compute column widths"""
        # set initial col. widths
        for col in self.columns:
            max_width = 0
            h_key = 'heading'
            if h_key in col:
                # use the heading
                max_width = len(col[h_key])
            else:
                # use the field name (or attribute)
                max_width = len(col['name'])
            col['max_width'] = max_width

        # now set max column widths
        for col in self.columns:
            for row in self.rows:
                width = self._width(row, col)
                if width > col['max_width']:
                    col['max_width'] = width

    def _decorated_row(self, row):
        key_list = self._sort_key_list()

        decorated_row = []
        for key in key_list:
            decorated_row.append(row[key])
        decorated_row.append(row)
        return decorated_row

    def _sort_key_list(self):
        """ produce a list for the sort key """
        key_list = []
        if isinstance(self.sort_key, str):
            key_list.append(self.sort_key)
        elif isinstance(self.sort_key, list):
            key_list = self.sort_key
        return key_list

    def _undecorated_row(self, row):
        """ remove the decorated row """
        return row[-1]

    def _add_decorated_rows(self):
        temp_rows = []
        for row in self.rows:
            temp_rows.append(self._decorated_row(row))

        self.rows = []
        for row in temp_rows:
            self.rows.append(self._undecorated_row(row))

    def __str__(self):
        """ printable table """
        if hasattr(self, 'sort_key'):
            # decorate
            self._add_decorated_rows()

        self._set_col_widths()

        table_strings = []
        # headings
        table_strings.append(self._stringify_col_headings())
        table_strings.append('\n')

        # heading table divider
        table_strings.append(self._stringify_table_divider())
        table_strings.append('\n')

        # content
        table_strings.append(self._stringify_table_content())
        table_strings.append('\n')

        return ''.join(table_strings)

    def _stringify_col_headings(self):
        """ return a string formatted to display column headings """
        heading_strings = []
        for col in self.columns:
            h_key = 'heading'
            col_heading = col[h_key] if h_key in col else col['name']
            heading_strings.append("{HEADING:{WIDTH}} ".format(
                HEADING=col_heading, WIDTH=col['max_width']))
        return ''.join(heading_strings)

    def _stringify_table_divider(self):
        divider_strings = []
        for col in self.columns:
            for i in range(col['max_width']):
                divider_strings.append('-')
            divider_strings.append('|')
        return ''.join(divider_strings)

    def _stringify_table_content(self):
        content_strings = []
        for row in self.rows:
            for col in self.columns:
                col_str = self._justify_col(col).format(
                    WIDTH=col['max_width'], COL_STR=self._col_str_dict(row, col))
                content_strings.append(col_str)
            # Remove any excess padding for the last column
            content_strings[-1].rstrip()
            content_strings.append('\n')
        return ''.join(content_strings)

    def _justify_col(self, col):
        """ 
        returns a formatting string for column based on its right_justify
        property
        """
        rjkey = 'right_justify'
        if rjkey in col and col[rjkey]:
            format_str = "{COL_STR:>{WIDTH}} "
        else:
            format_str = "{COL_STR:{WIDTH}} "
        return format_str
