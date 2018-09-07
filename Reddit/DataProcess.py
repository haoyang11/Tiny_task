import os
import pandas as pd
import datetime
import pickle


alldatalist=[]
print(os.listdir())
pickle_file = open('reftable_0906_all.pkl', 'rb')
ref_list = pickle.load(pickle_file)

def main():
    global  alldatalist
    dir_list=['rawdata']
    for file in  dir_list:
    	tsvfilelist = os.listdir(file)
    	print(file)
    	for tsvfile in tsvfilelist:
    		if "tsv" in tsvfile:
    			print(tsvfile)
    			add_reftime(tsvfile,file)
    save_file = open('rawdata.pkl', 'wb')
    pickle.dump(alldatalist, save_file)
    save_file.close()

def ratio_t(count, reftime):
	global ref_list
	rlist=[]
	llist=[]
	time_num=48
	ref_num=100
	baseratio=1.0/1000
	time_seg=int(reftime*1.0/30)
	ratio=1
	# print(time_seg)
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


def add_reftime(filename,path):
	global alldatalist
	path2=filename
	reftime=path2.split('.')[0]
	reftime=reftime.replace('T',' ')
	reftime=reftime.replace('_',':')
	try:
		df1 = pd.read_csv(os.path.join(path,path2), sep='\t',low_memory=False,header=None)
	except:
		return
	finally:
		pass
	
	df1= df1.dropna(axis=1,how='all')
	df1= df1.fillna('missing')
	try:
		fec_isnum=df1.iloc[:,8].str.isdigit()
		df1 = df1[fec_isnum].copy()
	except:
		pass
	finally:
		pass

	df1[8]=df1[8].astype(float)
	df1[11]=pd.to_datetime(df1[11])
	df1['score'] = df1.apply(lambda row: my_diff(row[8], row[9]), axis=1)
	df1['reftime']=reftime
	df1['reftime']=pd.to_datetime(df1['reftime'])
	df1['diff_time']=df1.apply(lambda row: my_difftime(row[11],row['reftime']), axis=1)
	df1['ratio_t']=df1.apply(lambda row: ratio_t(row['score'],row['diff_time']), axis=1)

	pd.set_option('display.max_columns', None)
	pd.set_option('display.max_rows', None)
	alldatalist.append(df1)



def my_diff(a, b):
	return float(a)-b

def my_difftime(creattime,reftime):
	return round((reftime-creattime).seconds/60)



if __name__=='__main__':
	main()
