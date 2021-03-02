from random import Random, randrange
import pandas as pd
import cx_Oracle
import config

# TODO: INSERT NULL
# TODO: DYNAMIC
# TODO GET TABLE

class DB:
    connection = None

    def __init__(self, landing_db='landing_db', relational_db='relational_db', s2t_mapping='s2t_mapping',
                 ref_dictionary='ref_dictionary'):
        self.landing_db = landing_db
        self.relational_db = relational_db
        self.s2t_mapping = s2t_mapping
        self.ref_dictionary = ref_dictionary
        print('DB...')
        try:

            self.connection = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                                             config.password,
                                                                             config.dsn,
                                                                             config.port,
                                                                             config.SERVICE_NAME))

            cx_Oracle.connect

            print('VERSION::', self.connection.version)


        except cx_Oracle.Error as error:
            print('ERROR', error)
            # release the connection
            if self.connection:
                self.connection.close()

    def printDescription(self):
        print('-------------------landing_db------------------------')

        sql = """SELECT * FROM {0}""".format(self.landing_db)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        for each in cursor.description:
            print(each[0:2])
        for each in cursor.execute(sql):
            print(each)

        print('-------------------landing_db------------------------')

        print('---------------------relational_db---------------------')

        sql = """SELECT * FROM {0}""".format(self.relational_db)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        c = 0
        for each in cursor.description:
            print(each[0:2])

        print('---------------------relational_db---------------------')

        print('---------------------s2t_mapping---------------------')

        sql = """SELECT * FROM {0}""".format(self.s2t_mapping)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        c = 0
        for each in cursor.description:
            print(each[0:2])

        print('---------------------s2t_mapping---------------------')

        print('---------------------ref_dictionary---------------------')

        sql = """SELECT * FROM {0}""".format(self.ref_dictionary)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        c = 0
        for each in cursor.description:
            print(each[0:2])

        print('---------------------ref_dictionary---------------------')

    # def getRowByNumber(self, rowNumber='1'):
    #     sql = """with cte as (select relational_db.*, ROW_NUMBER() OVER (ORDER BY batch_id) R from relational_db) select * from cte where R ={0}""".format(
    #         rowNumber)
    #     print(':::::', sql)
    #     cursor = self.connection.cursor()
    #     for each in cursor.execute(sql):
    #         print(each)
    #         return each
    #
    # def getRowByNumber2(self, rowNumber='1'):
    #     sql = """SELECT * FROM relational_db"""
    #     print(':::::', sql)
    #     cursor = self.connection.cursor()
    #     index = 1
    #     for each in cursor.execute(sql):
    #         print(each)
    #         if index == rowNumber:
    #             return each
    #         index += 1

    def createS2TMappingTable(self):
        sql = """
      CREATE TABLE EPUBLICATION.{0}
  ( Sheet_Source VARCHAR2(4000 CHAR),
    Cell_Source VARCHAR2(4000 CHAR),
    SHEET_TARGET VARCHAR2(200 CHAR),
    CELL_TARGET VARCHAR2(200 CHAR),
    CELL_TYPE VARCHAR2(200 CHAR),
    DESC_AR VARCHAR2(4000 CHAR),
    DATA_TYPE VARCHAR2(4000 CHAR),
    IS_MANDATORY VARCHAR2(200 CHAR),
    REF_DICTIONARY VARCHAR2(200 CHAR)
   )
""".format(self.s2t_mapping)
        print(':::::', sql)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            self.connection.commit()
            print(' A NEW TABLE WITH THE FOLLOWING NAME:', str(self.s2t_mapping),
                  ' HAS BEEN CREATED')
        except Exception as e:
            if str(e).__contains__('name is already used by an existing object'):
                print('TABLE NAME ALREADY EXISTS...')
                newTableNumber = '_' + str(randrange(0, 10))
                print('CREATING A NEW TABLE WITH THE FOLLOWING NAME:', str(self.s2t_mapping + newTableNumber))
                sql = sql.replace(self.s2t_mapping, self.s2t_mapping + newTableNumber)
                cursor.execute(sql)
                self.connection.commit()
                print(' A NEW TABLE WITH THE FOLLOWING NAME:', str(self.s2t_mapping + newTableNumber),
                      ' HAS BEEN CREATED')
                self.s2t_mapping += str(newTableNumber)
                return str(newTableNumber)
            else:
                print(e)

    def createRelationalDBTable(self):
        sql = """
          CREATE TABLE EPUBLICATION.{0}
      ( PUBLICATION_NAME_AR VARCHAR2(4000 CHAR),
        PUBLICATION_NAME_EN VARCHAR2(4000 CHAR),
        PUBLICATION_DATE_AR VARCHAR2(200 CHAR),
        PUBLICATION_DATE_EN VARCHAR2(200 CHAR),
        TABLE_ID VARCHAR2(200 CHAR),
        REP_NAME_AR VARCHAR2(4000 CHAR),
        REP_NAME_EN VARCHAR2(4000 CHAR),
        TEM_ID VARCHAR2(200 CHAR),
        CL_AGE_GROUP_AR_V1 VARCHAR2(200 CHAR),
        CL_AGE_GROUP_EN_V1 VARCHAR2(200 CHAR),
        CL_SEX_AR_V1 VARCHAR2(200 CHAR),
        CL_SEX_EN_V2 VARCHAR2(200 CHAR),
        OBS_VALUE VARCHAR2(200 CHAR),
        TIME_PERIOD_Y VARCHAR2(200 CHAR),
        TIME_PERIOD_M VARCHAR2(200 CHAR),
        NOTE1_AR VARCHAR2(4000 CHAR),
        NOTE1_EN VARCHAR2(4000 CHAR),
        NOTE2_AR VARCHAR2(4000 CHAR),
        NOTE2_EN VARCHAR2(4000 CHAR),
        NOTE3_AR VARCHAR2(4000 CHAR),
        NOTE3_EN VARCHAR2(4000 CHAR),
        SOURCE_AR VARCHAR2(200 CHAR),
        SOURCE_EN VARCHAR2(200 CHAR),
        TIME_STAMP VARCHAR2(200 CHAR),
        BATCH_ID VARCHAR2(200 CHAR),
        FREQUENCY VARCHAR2(200 CHAR)
       )
    """.format(self.relational_db)
        print(':::::', sql)
        cursor = self.connection.cursor()
        try:
            cursor.execute(sql)
            self.connection.commit()
            print(' A NEW TABLE WITH THE FOLLOWING NAME:', str(self.relational_db),
                  ' HAS BEEN CREATED')
        except Exception as e:
            if str(e).__contains__('name is already used by an existing object'):
                # gggg
                print('TABLE NAME ALREADY EXISTS...')
                newTableNumber = '_' + str(randrange(0, 10))
                print('CREATING A NEW TABLE WITH THE FOLLOWING NAME:', str(self.relational_db + newTableNumber))
                sql = sql.replace(self.relational_db, self.relational_db + newTableNumber)
                cursor.execute(sql)
                self.connection.commit()
                print(' A NEW TABLE WITH THE FOLLOWING NAME:', str(self.relational_db + newTableNumber),
                      ' HAS BEEN CREATED')
                self.relational_db += str(newTableNumber)
                return str(newTableNumber)
            else:
                print(e)

    def insertIntoRef_dictionary(self, DESCRIPTION='', ID='', CL_ID=''):
        sql = """INSERT INTO {3} (DESCRIPTION,ID,CL_ID)
        values ('{0}','{1}','{2}')""".format(DESCRIPTION, ID, CL_ID, self.ref_dictionary)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()

    def insertIntoS2t_mapping(self, sheetSource='', cellSource='', SHEET_TARGET='', CELL_TARGET='', CELL_TYPE='',
                              DESC_AR='', DATA_TYPE='', IS_MANDATORY='', REF_DICTIONARY=''):
        sql = """INSERT INTO {9} (Sheet_Source,Cell_Source,SHEET_TARGET,CELL_TARGET,CELL_TYPE, DESC_AR , DATA_TYPE , IS_MANDATORY , REF_DICTIONARY)
        values ('{0}','{1}','{2}','{3}','{4}', '{5}' ,'{6}','{7}','{8}')""".format(sheetSource, cellSource,
                                                                                   SHEET_TARGET, CELL_TARGET, CELL_TYPE,
                                                                                   DESC_AR, DATA_TYPE, IS_MANDATORY,
                                                                                   REF_DICTIONARY, self.s2t_mapping)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()

    def insertIntoLandingDB(self, sheetSource='', cellSource='', cellContent='', TimeStamp='', BatchID=''):

        sql = """INSERT INTO {5} (Sheet_Source,Cell_Source,Cell_Content,Time_Stamp,Batch_ID)
        values ('{0}','{1}','{2}','{3}','{4}' )""".format(sheetSource, cellSource, cellContent,
                                                          TimeStamp, BatchID, self.landing_db)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql)
        self.connection.commit()

    def insertIntoRelationalDB(self,
                               PUBLICATION_NAME_AR,
                               PUBLICATION_NAME_EN,
                               PUBLICATION_DATE_AR,
                               PUBLICATION_DATE_EN,
                               TABLE_ID,
                               REP_NAME_AR,
                               REP_NAME_EN,
                               TEM_ID,
                               CL_AGE_GROUP_EN_V1,
                               CL_AGE_GROUP_AR_V1,
                               CL_SEX_AR_V1,
                               CL_SEX_EN_V2,
                               OBS_VALUE,
                               TIME_PERIOD_Y,
                               TIME_PERIOD_M,
                               NOTE1_AR,
                               NOTE1_EN,
                               NOTE2_AR,
                               NOTE2_EN,
                               NOTE3_AR,
                               NOTE3_EN,
                               SOURCE_AR,
                               SOURCE_EN,
                               TIME_STAMP, Batch_ID, FREQUENCY):

        sql = """insert into {0} ( PUBLICATION_NAME_AR, PUBLICATION_NAME_EN, PUBLICATION_DATE_AR, 
        PUBLICATION_DATE_EN, TABLE_ID, REP_NAME_AR, REP_NAME_EN, TEM_ID, CL_AGE_GROUP_AR_V1, CL_AGE_GROUP_EN_V1, CL_SEX_AR_V1, CL_SEX_EN_V2, 
        OBS_VALUE, TIME_PERIOD_Y, TIME_PERIOD_M, NOTE1_AR, NOTE1_EN, NOTE2_AR, NOTE2_EN, NOTE3_AR, NOTE3_EN, SOURCE_AR, SOURCE_EN, TIME_STAMP, BATCH_ID, FREQUENCY) values (
        :1 , :2 , :3 , :4 , :5 , :6 , :7 , :8 ,:9 ,:10 ,:11 ,:12 ,:13 ,:14 ,:15 ,:16 ,:17 ,:18 ,:19 ,:20 ,:21 ,:22 ,:23 ,:24 ,:25 , :26)""".format(
            self.relational_db)
        print(':::::', sql)
        cursor = self.connection.cursor()
        cursor.execute(sql, [PUBLICATION_NAME_AR,
                             PUBLICATION_NAME_EN,
                             PUBLICATION_DATE_AR,
                             PUBLICATION_DATE_EN,
                             TABLE_ID,
                             REP_NAME_AR,
                             REP_NAME_EN,
                             TEM_ID,
                             CL_AGE_GROUP_AR_V1,
                             CL_AGE_GROUP_EN_V1,
                             CL_SEX_AR_V1,
                             CL_SEX_EN_V2,
                             OBS_VALUE,
                             TIME_PERIOD_Y,
                             TIME_PERIOD_M,
                             NOTE1_AR,
                             NOTE1_EN,
                             NOTE2_AR,
                             NOTE2_EN,
                             NOTE3_AR,
                             NOTE3_EN,
                             SOURCE_AR,
                             SOURCE_EN,
                             TIME_STAMP, Batch_ID, FREQUENCY, ])
        self.connection.commit()

    def printRelationalDB(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))

        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.relational_db)
        cursor.execute(SQL)
        for record in cursor:
            print('relational_db', record)

    def printLandingDB(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))
        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.landing_db)
        cursor.execute(SQL)
        for record in cursor:
            print('landing_db', record)

    def printS2t(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))
        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.s2t_mapping)
        cursor.execute(SQL)
        for record in cursor:
            print('s2t_mapping', record)

    def printRefDictionary(self):
        db = cx_Oracle.connect('{0}/{1}@{2}:{3}/{4}'.format(config.username,
                                                            config.password,
                                                            config.dsn,
                                                            config.port,
                                                            config.SERVICE_NAME))
        cursor = db.cursor()
        SQL = "SELECT * FROM {0}".format(self.ref_dictionary)
        cursor.execute(SQL)
        for record in cursor:
            print('ref_dictionary', record)

    def tamarPandas(self):
        SQL = """SELECT * FROM {0}""".format(self.relational_db)
        df_input = pd.read_sql(SQL, con=self.connection)
        return df_input

    def closeConnection(self):
        # release the connection
        if self.connection:
            self.connection.close()

