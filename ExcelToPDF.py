import win32com.client
import os
from datetime import datetime
from random import randint


def excelToPDF(batchID='', fileName='', sheetsListToConvert=['Target Table']):
    #  [4, 6, 7, 8, 9]
    o = win32com.client.Dispatch("Excel.Application")

    o.Visible = False

    directory = os.path.abspath('.')

    wb = o.Workbooks.Open(directory + "/{0}".format(fileName))


    ws_index_list = sheetsListToConvert

    counter = 1
    print('===========================SAVING PDF')
    for item in ws_index_list:
        path_to_pdf = directory + '/{0}_{1}.pdf'.format(item,batchID)
        if type(item) == type(''):
            wb.sheets(item).Select()
        elif type(item) == type(1):
            wb.WorkSheets(item).Select()
        wb.ActiveSheet.ExportAsFixedFormat(0, path_to_pdf)
        print('SAVING: {0}'.format(path_to_pdf))
        counter += 1

    wb.Close()


