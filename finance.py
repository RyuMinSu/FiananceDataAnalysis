#%%import numpy as np
import pandas as pd

import FinanceDataReader as fdr

import pymysql
from sqlalchemy import create_engine



#%%
def creattb(ctsql): #테이블생성
    cur.execute(ctsql)

def uploadtb(df, ctsql, insql): #테이블생성 + 값채우기
    creattb(ctsql)
    print("table 저장 완료")
    for i in range(len(df)):
        val = df.iloc[i].values.tolist()
        print(val)
        cur.execute(insql, val)



#%%
krxDf = fdr.StockListing("KRX") #데이터 불러오기
print("krxDf shape:", krxDf.shape)

krxDf.columns = krxDf.columns.str.lower()

df1 = krxDf[["code", "isu_cd", "name"]]
df2 = krxDf[["code", "marketid", "market"]]
df3 = krxDf.drop(["isu_cd", "name", "market", "dept", "marketid"], axis=1)
print(f"df1 shape: {df1.shape}")
print(f"df2 shape: {df2.shape}")
print(f"df3 shape: {df3.shape}")

conn = pymysql.connect(host="", user="", passwd="", db="", charset="") #db저장
cur = conn.cursor()

ctsql1 = "create table company(code varchar(10) primary key, cd varchar(20), name varchar(20))"
ctsql2 = "create table market(code varchar(10), marketid varchar(10), market varchar(30),\
    foreign key(code) references company(code) on update cascade)"
ctsql3 = "create table stock(code varchar(10), close float, changecode float, changes float,\
    changesratio float, open float, high float, low float, volume float, amount float,\
        marcap float, stocks float, foreign key(code) references company(code) on update cascade)"

insql1 = "insert into company values(%s, %s, %s)"
insql2 = "insert into market values(%s, %s, %s)"
insql3 = "insert into stock values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

uploadtb(df1, ctsql1, insql1)
uploadtb(df2, ctsql2, insql2)
uploadtb(df3, ctsql3, insql3)

conn.commit()
conn.close()
# %%
