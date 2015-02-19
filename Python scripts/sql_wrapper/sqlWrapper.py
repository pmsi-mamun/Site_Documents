__author__ = 'atalhan'
import pyodbc, logging


class SQLWrapper:
    """
    Wrapper class to expose simple sql api
    """
    con = {}
    cursor = {}
    logger = logging.getLogger('sql_logger')

    def __init__(self, server, db, integrated_security, user, password, logger, driver='SQL Server'):
        connection_str = ''
        if integrated_security:
            connection_str = 'DRIVER={0};SERVER={1};DATABASE={2};Integrated Security=SSPI'\
                .format(driver, server, db)
        else:
            connection_str = 'DRIVER={0};SERVER={1};DATABASE={2};UID={3};PWD={4}'\
                .format(driver, server, db, user, password)

        self.con = pyodbc.connect(connection_str)
        self.cursor = self.con.cursor()
        self.logger = logger

    def get_rows_for_qry(self, qry):
        """
        :param qry:
        :return list of rows for given query:
        """
        results = []
        self.cursor.execute(qry)

        while 1:
            row = self.cursor.fetchone()
            if not row:
                break
            results.append(row)

        return results

    def get_objects_for_qry(self, qry):
        """
        :param qry:
        :return list of rows for given query:
        """
        results = self.get_rows_for_qry(qry)
        counter = 0

        first_row = results[0];
        rtn_objs = []

        for row in results:
            obj = {}
            counter = 0
            while counter < len(first_row):
                obj[row.cursor_description[counter][0]] = row[counter]
                counter += 1

            rtn_objs.append(obj)

        return rtn_objs

    def get_todays_completed_sweeps(self, scraper_type):
        """
        :param scraper_type:
        :return Completed sweeps for today:
        """
        completedSweeps = []
        self.cursor.execute(
            "select "
            "    ss.Name as Site, SweepId, s.CreatedOn"
            " from sweep s (nolock)"
            " inner join ScraperSite ss (nolock) on ss.ScraperSiteId = s.ScraperSiteId"
            " inner join ScraperSiteMeta ssm (nolock) on ss.ScraperSiteId = ssm.ScraperSiteId and ssm.Metafield = 'SiteType'"
            " where s.isdeleted = 0 and s.iscompleted = 1 and progress = 100 "
            "    and s.createdOn >= dateAdd(dd,0,datediff(day,0,getdate()))"
            "    and ssm.DefaultValue = '"+scraper_type + "'")

        while 1:
            row = self.cursor.fetchone()
            if not row:
                break
            completedSweeps.append(row.SweepId)

        return completedSweeps


    def get_todays_non_validated_sweeps(self):
        """
        :returns Completed and not validated sweeps for today:
        """
        completedSweeps = []
        self.cursor.execute(
            "select "
            "    s.SweepId, ss.Name as Site"
            " from Sweep s (nolock)"
            " inner join ScraperSite ss (nolock) on ss.ScraperSiteId = s.ScraperSiteId"
            " inner join ScraperSiteMeta ssm (nolock) on ss.ScraperSiteId = ssm.ScraperSiteId and ssm.Metafield = 'DataValidation'"
            " left join SweepMetaData smd (nolock) on smd.SweepId = s.SweepId and smd.MetaField ='Validation_Status'"
            " where s.IsDeleted =0 and s.IsCompleted =1 and s.Progress =100 "
            "    and DATEDIFF(hour,s.CreatedOn, getdate()) <= 24"
            "    and ssm.DefaultValue ='true' and smd.MetaFieldValue is null")

        while 1:
            row = self.cursor.fetchone()
            if not row:
                break
            completedSweeps.append(row)

        return completedSweeps

    def execute_stored_procedure(self, proc_name, parameters):
        """
        :param proc_name: name of the stored procedure
        :parameters : comma seperated list of parameters
        :return list of rows for given query: list of rwos if proc returns any
        """
        results = []
        query = "execute "+proc_name+" "+parameters
        self.cursor.execute(query)

        if self.cursor.description is not None:
            while 1:
                row = self.cursor.fetchone()
                if not row:
                    break
                results.append(row)

        first_row = results[0];
        rtn_objs = []

        for row in results:
            obj = {}
            counter = 0
            while counter < len(first_row):
                obj[row.cursor_description[counter][0]] = row[counter]
                counter += 1

            rtn_objs.append(obj)

        self.con.commit()
        return rtn_objs


    def verify_table(self, name, counter):
        """
        Verifies if a table exists if user has the permissions
        :param name:
        :param counter:
        :return:
        """
        self.cursor.execute("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'dbo'  "
                            "AND  TABLE_NAME = '%s'", name)
        exists = False
        while 1:
            row = self.cursor.fetchone()
            if not row:
                break
            exists = True

        if not exists:
            return name + str(counter)
        else:
            return self.verify_table(name,counter+1)

    def create_table(self, name, fields):
        """
        :param name:
        :param fields:
        :return:
        """
        str_sql = "create table [dbo].[" + name + "] ("
        first = True
        for field in fields:
            if not first:
                str_sql += ","

            str_sql += "[" + field + "] [varchar] (max)"
            first = False

        str_sql += " )"
        self.logger.info(str_sql)
        response = self.cursor.execute(str_sql).rowcount
        self.logger.info(response)

    def insert_data_table(self,table, fields, data):
        """
        :param table:
        :param fields:
        :param data:
        :return:
        """
        for d in data:
            insert_sql = "insert into " + table + " ("
            first = True
            for f in fields:
                if not first:
                    insert_sql += ","

                insert_sql += f
                first = False

            insert_sql += ") values("

            first = True
            for f in d:
                if not first:
                    insert_sql += ","

                insert_sql += "'" + f + "'"

            self.cursor.execute(insert_sql)

    def insert_data_table_1line(self,table, fields, data):
        """
        fgas
        :param table: string type : "[dbo].[patati_and_patata]"
        :param fields: list type on strings : ["field1","fleed2","flight3"]
        :param data: a list type of string : one can insert a number of function e.g. data = [str(199997),"'Validation_Status'","'Validating'",'getdate()'] will insert a list of [integer, string, string, function]
        :return:
        """

        insert_sql = "insert into " + table + " ("
        first = True
        for f in fields:
            if not first:
                insert_sql += ","

            insert_sql += f
            first = False

        insert_sql += ") values("

        first = True
        for f in data:
            if not first:
                insert_sql += ","

            insert_sql += f
            first = False
        insert_sql += ")"
        print(insert_sql)
        self.cursor.execute(insert_sql)
        self.con.commit()
        return 1

