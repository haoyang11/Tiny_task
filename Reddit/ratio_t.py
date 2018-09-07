import os
import pandas as pd
import datetime
import pickle
from pandas.core.frame import DataFrame


pickle_file = open('reftable_0906_all.pkl', 'rb')
ref_list = pickle.load(pickle_file)


def ratio_t(count, reftime):
	global ref_list
	rlist=[]
	llist=[]
	time_num=48
	ref_num=100
	baseratio=1.0/1000
	time_seg=int(reftime*1.0/30)
	ratio=1
	print(time_seg)
	if time_seg>time_num-1:
		for i in range(ref_num):
			basecount =ref_list[time_num-1][i] 
			if  basecount < count:
				ratio= i* baseratio
				break
		return ratio

	if time_seg==0:
		for i in range(ref_num):
			basecount=ref_list[0][i]*reftime/30.0
			if  basecount < count:
				ratio= i* baseratio
				break
		return ratio

	rlist=ref_list[time_seg]
	llist=ref_list[time_seg-1]
	ratio=1
	for i in range(ref_num):
		ref_t= (int(reftime)%30)*1.0/30
		basecount= llist[i]*(1-ref_t)+rlist[i]*ref_t
		if basecount < count:
			ratio= i * baseratio
			break
	return ratio



def my_diff(a, b):
	return float(a)-b

def my_difftime(creattime,reftime):
	# ct=datetime.datetime(creattime)
	# reftime=creattime.date()+datetime.timedelta(days=1)
	# reftime=datetime.datetime(reftime)
	# reftime=reftime+datetime.timedelta(days=1)
	# print(creattime,reftime)
	# print(round((reftime-creattime).seconds/60))
	return round((reftime-creattime).seconds/60)


def main():
	t=ratio_t(100, 59)
	print(t)

	filename="rawdata.csv"
	df1 = pd.read_csv(filename, sep=',',low_memory=False,header=None)
	df1= df1.dropna(axis=1,how='all')
	df1= df1.fillna('null')

	fec_isnum=df1.iloc[:,8].str.isdigit()
	df1 = df1[fec_isnum].copy()

	print(df1.dtypes)

	df1['count'] = df1.apply(lambda row: my_diff(row[8], row[9]), axis=1)

	df1[11]=pd.to_datetime(df1[11])
	df1['diff_time']=df1.apply(lambda row: my_difftime(row[11],row['reftime']), axis=1)
	df1['ratio_t']=df1.apply(lambda row: ratio_t(row['count'],row['diff_time']), axis=1)

	# df1['ratio_rank']=df1.groupby([15,13,16])['ratio_t'].rank(ascending=0,method='first')
	# df1=df1[['count','diff_time']]
	# print(df1.head())

	df1.to_csv("ratio_rank_new_0906.csv")

	df1.to_csv("ratio_rank_new_0906.tsv",sep='\t',header=None,index=False)



if __name__=='__main__':
	main()


