import win32com.client
import os
from datetime import datetime
from random import randint


def excelToPDF(batchID='', fileName='' , sheetsListToConvert = [ 'Target Table' , 'Target Graph' ,'Target PT' ,  'Text'] ):
    #  [4, 6, 7, 8, 9]
    o = win32com.client.Dispatch("Excel.Application")

    o.Visible = False

    directory = os.path.abspath('.')

    wb = o.Workbooks.Open(directory + "/" + fileName)

    ws_index_list = sheetsListToConvert  # say you want to print these sheets

    counter = 1

    for item in ws_index_list:
        path_to_pdf = directory + '/Sample{0}_{1}.pdf'.format(batchID, counter)
        if type(item) == type(''):
            wb.WorkSheets(item).Select()
        elif type(item) == type(1):
            wb.sheets(item).Select()
        wb.ActiveSheet.ExportAsFixedFormat(0, path_to_pdf)
        counter += 1

    wb.Close()