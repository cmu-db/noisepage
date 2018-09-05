#!/usr/bin/python

import getopt
import sys
from types import *

class TextTable:
    # headings = dict
    #     key (string)
    #     value = text heading to use
    #
    # data = list of dictionaries
    #     key (string)
    #     value = text

    def __init__(self):
        self.rows = []
        # Columns to print, each item is a dictinary
        self.columns = []
        return

    def add_row(self, item):
        """ item - dictionary or object with attributes
        """
        self.rows.append( item )
        return

    def add_column(self, column, heading=None, format=None,
                  right_justify=False):
        """ Add single column (by name), to be printed
        """
        dict = {}
        dict['name'] = column
        if format:
            dict['format'] = format
        if heading:
            dict['heading'] = heading
        if right_justify:
            dict['right_justify'] = True
        self.columns.append( dict )
        return

    def sort(self, sort_spec):
        """Sort, single field or list of fields"""
        # remember the field name, and sort prior to output
        self.sort_key = sort_spec
        return

    def _width(self, row, col):
        if type(row) == DictType:
            return self._width_dict( row, col)
        return self._width_obj( row, col)

    def _col_str(self, row, col):
        if type(row) == DictType:
            return self._col_str_dict( row, col)
        return self._col_str_obj( row, col)

    def _col_str_dict(self, row, col):
        """ Return printable field (dictionary) """
        field = col['name']
        if not row.has_key( field ):
            return u""
        if col.has_key( 'format' ):
            return col['format'] % row[field]
        return u"{}".format(row[field])

    def _col_str_obj(self, row, col):
        """ Return printable field (object)"""
        field = col['name']
        if not hasattr(row, field):
            return u""
        if col.has_key( 'format' ):
            try:
                value = getattr(row, field)
                if value == None:
                    return u"None"
                return col['format'] % value
            except:
                return u"????"

        return u"%s" % getattr(row, field)

    def _width_dict(self, *args):
        """ Return width of field (dictionary) """
        return len(self._col_str_dict(*args))

    def _width_obj(self, *args):
        """ Return width of field (object)"""
        return len(self._col_str_obj(*args))

    def _col_widths(self):
        """ Compute column widths"""
        # set initial col. widths
        for col in self.columns:
            max_width = 0
            hkey = 'heading'
            if col.has_key( hkey ):
                # use the heading
                max_width = len(col[ hkey ])
            else:
                # use the field name (or attribute)

                max_width = len( col['name'] )
            col['max_width'] = max_width

        # now set max column widths
        for col in self.columns:
            for row in self.rows:
                width = self._width( row, col )
                # print "width of %s is %d" % (col['name'], width)
                if width > col['max_width']:
                    col['max_width'] = width

    # ----
    # sorting support

    def _sort_key_list(self):
        # produce a list for the sort key, even if it is a single
        # sort
        key_list = []
        if type(self.sort_key) == StringType:
            key_list.append(self.sort_key)
        elif type(self.sort_key) == ListType:
            key_list = self.sort_key
        return key_list

    def _decorated_row(self, row):
        key_list = self._sort_key_list()

        ret_val = []
        # is the row a dictionary or object?
        if type(row) == DictType:
            for key in key_list:
                ret_val.append( row[key] )
        else:
            for key in key_list:
                ret_val.append( getattr(row, key) )
        ret_val.append(row)
        return ret_val

    def _undecorated_row(self, row):
        return row[-1]

    def __str__(self):
        if hasattr(self, 'sort_key'):
            # decorate
            temp_rows = []
            for row in self.rows:
                temp_rows.append(self._decorated_row(row))
            temp_rows.sort()

            self.rows = []
            for row in temp_rows:
                self.rows.append(self._undecorated_row(row))

        self._col_widths()

        # headings
        ret_str = u""
        for col in self.columns:
            hkey = 'heading'
            if col.has_key( hkey ):
                col_heading = col[ hkey ]
            else:
                col_heading = col[ 'name' ]
            ret_str = ret_str +  u"%-*s " % ( col['max_width'],
                                              col_heading )
            #print "col: name", col['name']
            #print "     widt", col['max_width']
            #print "     head", "'%s'" % (col_heading)

        ret_str = ret_str + u"\n"

        for row in self.rows:
            for col in self.columns:
                rjkey = 'right_justify'
                if col.has_key( rjkey ) and col[rjkey]:
                    format_st = u"%*s "
                else:
                    format_st = u"%-*s "
                ret_str = ret_str +  format_st % (
                    col['max_width'], self._col_str(row, col))

            # Remove any excess padding. For the last column, we don't
            # need the padding for the column width. Remove it.
            ret_str = ret_str.rstrip()

            ret_str = ret_str + u"\n"
        return ret_str

# ----

def usage():
    return

if __name__ == "__main__":

    try:
        opts, args = getopt.getopt(sys.argv[1:], "h")
    except getopt.GetoptError:
        # print help information
        usage()
        sys.exit(2)

    tt = TextTable()
    tt.addRow( { 'email' : 'abc@aol.com',
                 'count' : 2 } )
    tt.addRow( { 'email' : 'abc@aol.com',
                 'count' : 1 } )
    tt.addRow( { 'email' : 'cdf@aol.com',
                 'count' : 0 } )
    tt.addColumn( 'email' )
    tt.addColumn( 'count', right_justify=True )
    tt.sort(['email', 'count'])
    print tt
