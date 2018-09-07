import pymssql
import pandas as pd


class MSSQL:

    def __init__(self):

        server="*.database.windows.net"
        user="**"
        password="*"
        database="reddit-westus"

        
        self.host = server
        self.user = user
        self.pwd = password
        self.db = database

    def __GetConnect(self):
        # return curson if success
        if not self.db:
            raise(NameError,"没有设置数据库信息")
        self.conn = pymssql.connect(host=self.host,user=self.user,password=self.pwd,database=self.db,charset="utf8")
        cur = self.conn.cursor(as_dict=True)
        if not cur:
            raise(NameError,"连接数据库失败")
        else:
            return cur

    def ExecQuery(self,sql):
        """
        执行查询语句
        返回的是一个包含tuple的list，list的元素是记录行，tuple的元素是每行记录的字段
        调用示例：
                ms = MSSQL(host="localhost",user="sa",pwd="123456",db="PythonWeiboStatistics")
                resList = ms.ExecQuery("SELECT id,NickName FROM WeiBoUser")
                for (id,NickName) in resList:
                    print str(id),NickName
        """
        cur = self.__GetConnect()
        df = pd.read_sql(sql,self.conn)
        df.to_csv("test.csv")
        df.head(5)
        cur.execute(sql)
        resList = cur.fetchall()
        self.conn.close()
        return resList

    def ExecNonQuery(self,sql):
        cur = self.__GetConnect()
        cur.execute(sql) 
        self.conn.commit()
        self.conn.close()

def main():

    ms = MSSQL()
    resList = ms.ExecQuery("SELECT TOP (100) [PostId],[PostLastModifiedDT],[IsAMA] FROM [dbo].[AMAs]")
    print("hello world")
    for var in resList:
        print(var)
    


if __name__ == '__main__':
    main()