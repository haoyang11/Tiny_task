import os
import pandas as pd
import datetime
import pickle
from pandas.core.frame import DataFrame


def main():
	result_path="result"

	pickle_file = open('rawdata.pkl', 'rb')
	alldata_list = pickle.load(pickle_file)
	alldata_frame=pd.concat(alldata_list)
	# alldata_frame.to_csv("rawdata.csv")
	print(alldata_frame.dtypes)

	# get new count
	newscounthead=['totalurl']
	newscount=alldata_frame[2].value_counts()
	# newscount['subreddit']=newscount.index
	newscount=pd.DataFrame(newscount)
	newscount.to_csv(os.path.join(result_path,"reddit_totalnewscount.csv"),header=newscounthead)
	
	newsvotehead=['totalvote']
	newsvote = alldata_frame.groupby(by=[2])['score'].sum()
	# newsvote['subreddit']=newsvote.index
	newsvote=pd.DataFrame(newsvote)
	newsvote.to_csv(os.path.join(result_path,"reddit_totalnewsvote.csv"),header=newsvotehead)
	
	result=pd.concat([newscount,newsvote], axis=1, join_axes=[newscount.index])

	# result = pd.merge(newscount,newsvote,how='inner',left_index=True,right_index=True)
	resultheader=['totalurl','totalvote']
	# result.to_csv(os.path.join(result_path,"reddit_statistic.csv"),header=resultheader)
	baseratio=0.005
	for i in range(1,20):
		ratio=i*baseratio
		headstr='ratio_'+str(ratio)+'_count'
		resultheader.append(headstr)
		ratio_frame=alldata_frame[alldata_frame['ratio_t']<ratio]
		ratiocount=ratio_frame[2].value_counts()
		ratiocount=pd.DataFrame(ratiocount)
		result=pd.concat([result,ratiocount], axis=1, join_axes=[result.index])

	result= result.fillna(0)
	result.to_csv(os.path.join(result_path,"reddit_ratio_statistic.csv"),header=resultheader)



if __name__ == '__main__':
	main()

