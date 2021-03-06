import pandas as pd
import numpy as np
import os
import glob
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from dbWrapper import dbWrapper

def get_name(i, df=None,date=False):
#fake overload
	if df is None:
		lhs,rhs=i.split("sp",1)
		if date==False:
			lhs,rhs=rhs.split("_",1)
			return lhs
		else:
			temp=rhs.split("_",2)
			return temp[1]
	else:
		row=df.iloc[[i]]
		name=row.index[0][0]
		lhs,rhs=name.split("sp",1)
		if date==False:
			lhs,rhs=rhs.split("_",1)
			return lhs
		else:
			temp=rhs.split("_",2)
			return temp[1]
			
			

def toString(date):
	if len(str(date.month))==1 and len(str(date.day))==1:
		return str(date.year)+'0'+str(date.month)+'0'+str(date.day)
	elif len(str(date.month))==1 and len(str(date.day))!=1:
		return str(date.year)+'0'+str(date.month)+str(date.day)
	else:
		return str(date.year)+str(date.month)+str(date.day)
	
dd = dbWrapper('192.168.10.188')

principal=500000.0
allFiles = glob.glob( "1705_1709/*c1*.csv")
spec=pd.read_csv("contract.csv")
# print spec.columns
if 'out.csv' in allFiles:
	os.remove('out.csv')
	allFiles.remove('out.csv')

name_list=[]
for name in allFiles:
	new_name=name[:-4]
	# lhs,rhs=new_name.split("sp",1)
	name_list.append(new_name)

colnames=["Buy","NA1","NA2","NA3","Date1","Time1","Price1","Date2","Time2","Price2"]
frame = pd.DataFrame()
list_ = []
for file_ in allFiles:
	df = pd.read_csv(file_,names=colnames)
	list_.append(df)
all_trades = pd.concat(list_,keys=name_list)
all_trades['Date1']+=19000000
all_trades['Date2']+=19000000
new_date=[]
new_date2=[]
for date in all_trades['Date1']:
	temp=str(date)
	new_date.append(datetime(year=int(temp[0:4]),month=int(temp[4:6]),day=int(temp[6:8])))
new_date2=[]
for date in all_trades['Date2']:
	temp=str(date)
	new_date2.append(datetime(year=int(temp[0:4]),month=int(temp[4:6]),day=int(temp[6:8])))

# for index, row in all_trades.iterrows():
	# date=str(row['Date1'])
	# time=str(row['Time1'])
	# new_date.append(datetime(year=int(date[0:4]),month=int(date[4:6]),day=int(date[6:8]),hour=int(time[1:3]),minute=int(time[4:6])))
	# date=str(row['Date2'])
	# time=str(row['Time2'])
	# new_date2.append(datetime(year=int(date[0:4]),month=int(date[4:6]),day=int(date[6:8]),hour=int(time[1:3]),minute=int(time[4:6])))
	
all_trades['Date1']=new_date
all_trades['Date2']=new_date2

contract=[]

for name in allFiles:
	lhs,rhs=name.split("sp",1)
	lhs,rhs=rhs.split("_",1)
	contract.append(lhs[0:])
all_contract=set(contract)

#construct dictionary for checking multiplier and expiration for different commodity
symbols=[]
for symbol in spec[['symbol']].values:
	temp=''.join([i for i in symbol[0] if not i.isdigit()])
	symbols.append(temp)
mult={}
for contract in list(all_contract):
	index=symbols.index(contract)
	mult[symbols[index]]=spec['multipler'][index]

	
#add new column of multiplier
multiplier=[]
all_commodity=set()
for index, row in all_trades.iterrows():
	name=get_name(index[0])
	name_with_date=get_name(index[0],date=True)
	multiplier.append(mult[name])
	all_commodity.add(name_with_date)

expiration={}

for commodity in list(all_commodity):
	commodity_with_month=commodity[:-4]+commodity[-2:]
	i=0
	for index, row in spec.iterrows():
		inst=row['instrumentid']
		if (inst[:-4]+inst[-2:]==commodity_with_month):
			i=index
			break
	# print spec.iloc[i]['expiredate']
	expire_date=str(spec.iloc[i]['expiredate'])
	expire_date=expire_date[4:]
	expiration[commodity_with_month]=expire_date


indices=all_trades.index.tolist()
N=len(indices)
settle_date={'15':'0101','59':'0501','91':'0901'}
amount=np.zeros(N)
latest_settle=np.zeros(N)

#calculate trading amount 
for i in range(N):
	if amount[i]==0:
	#if no amount was given
		trade_amount=int(principal/(all_trades['Price1'][i]*mult[get_name(i,all_trades)]))
		amount[i]=trade_amount
		trade_id=indices[i][1]
		#find the opposite leg by id match
		for j in range(i+1,N):
			if indices[j][1]==trade_id:
				opposite_leg=j
				break
		amount[opposite_leg]=trade_amount
	if latest_settle[i]==0:
		first_digit=(all_trades.index[i][0])[3]
		second_digit=(all_trades.index[i][0])[8]
		temp=settle_date[str(first_digit+second_digit)]
		latest_settle[i]=temp
		for j in range(i+1, N):
			if indices[j][1]==trade_id:
				opposite_leg=j
				break
		latest_settle[opposite_leg]=temp
		
all_trades['amount']=amount
all_trades['mult']=multiplier
all_trades['settle']=latest_settle
all_trades=all_trades.sort_values(by=['Date1','Time1'])
all_trades.to_csv('out.csv',index=True)




start_date=all_trades.iloc[0]['Date1']
end_date=all_trades.iloc[-1]['Date2']
duration=(end_date-start_date).days*24

last_row=all_trades.tail(2)
last_row=last_row.head(1)

if last_row['Date1'].values==last_row['Date2'].values and last_row['Time1'].values==last_row['Time2'].values:
	name_for_search=get_name(last_row.index[0][0],date=True)
	name_for_search=name_for_search[:-4]+name_for_search[-2:]
	expire_date=expiration[name_for_search]
	ns = 1e-9
	date_end=datetime.utcfromtimestamp(last_row['Date1'].values[0].astype(datetime)*ns)
	date_end=datetime(year=date_end.year,month=int(expire_date[:2]),day=1)
	end_date=date_end-timedelta(days=2)
	duration=(end_date-start_date).days*24
print start_date,end_date, (end_date-start_date).days*24


#dictionary of all price data
all_prices={}
for commodity in all_commodity:
		date1=toString(start_date)
		date2=toString(end_date)
		price_night=dd.getNightBars(commodity,date1,date2,'1 Hour')
		price_day=dd.getBars(commodity,date1,date2,'1 Hour')
		if not price_day.empty:
			price_day=price_day.iloc[:,[0,1,5]]
		if not price_night.empty:
			price_night=price_night.iloc[:,[0,1,5]]
		# print price_day
		# print price_night
		prices=pd.concat([price_night,price_day]).sort_values(by=['tradingday','bartime'],ascending=[True,True])
		all_prices[commodity]=prices





# Here are the main matrices we're interested in. Value to date gives daily value of according to the pl matrix. Each line represents different
# contract. Contracts of same commodity with different expiration date are considered as seperate contracts. 
value_to_date=np.zeros([len(all_commodity),duration+2])
pl=np.zeros([len(all_commodity),duration+1])

value_to_date[:,0]=principal

base = start_date
date_list = [base + timedelta(hours=x) for x in range(0, duration+1)]

for index, row in all_trades.iterrows():
	commodity_name=get_name(index[0],date=True)
	commodity_index=list(all_commodity).index(commodity_name)
	amount_traded=row['Buy']*row['amount']
	date1=toString(row['Date1'])
	date2=toString(row['Date2'])
	price0=row['Price1']
	price_end=row['Price2']
	hold=0
	if (date1==date2) and (row['Time1']==row['Time2']):
		name_for_search=commodity_name[:-4]+commodity_name[-2:]
		expire_date=expiration[name_for_search]
		expire_date=datetime(year=row['Date2'].year,month=int(row['settle']/100.0),day=1)
		if int(toString(expire_date))>int(toString(end_date)):
			expire_date=end_date
		date2=toString(expire_date)
		price_end=0
		hold=1
	data=[(ind, rows) for (ind, rows) in all_prices[commodity_name].iterrows() if ((rows['tradingday']>=date1) and (rows['tradingday']<=date2 ) ) ]
	print len(data)
	if len(data)>0:
		# print data[0][1].loc['tradingday']
		prices=[price0,data[0][1].loc['closeprice']]
		if hold:
			price_end=data[-1][1].loc['closeprice']
		for i in range(1,len(data)):
			prices.append(data[i][1].loc['closeprice'])
		# prices.append(price_end)
		price_diff=np.diff(prices)
		
		trading_time=[]
		for i in range(len(data)):
			date=data[i][1].loc['tradingday']
			time=data[i][1].loc['bartime']
			trading_time.append(str(date)+str(time))
		trading_time.append(date2+'12:00')
		
		# print trading_days
		time_index=[]
		for day in trading_time:
			day_date=datetime(year=int(day[0:4]),month=int(day[4:6]),day=int(day[6:8]),hour=int(day[8:10]),minute=int(day[11:13]))
			time_index.append(int((day_date-start_date).seconds/3600.0)+(day_date-start_date).days*24)
		# date_index=[x+1 for x in date_index]
		# set up pl matrix
		for i in range(len(price_diff)):
			pl[commodity_index,time_index[i]]+=price_diff[i]*amount_traded*row['mult']
		for i in range(len(pl[commodity_index,:])):
			value_to_date[commodity_index,i+1]=value_to_date[commodity_index,i]+pl[commodity_index,i]
		
		

# print pl	
net_worth=value_to_date/principal
net_worth_sum=np.sum(net_worth,axis=0)/len(all_commodity)
data_point=[(date,worth) for date,worth in zip(date_list,net_worth_sum) ]
# print len(date_list) ,duration 
pl_df={"date": date_list, "pl":np.sum(pl,axis=0)}
df = pd.DataFrame(data=pl_df)
print df.shape
df.to_csv('pl.csv')


x_axis=[]
y_axis=[]
for date,worth in data_point:
	x_axis.append(date)
	y_axis.append(worth)
plt.plot(x_axis,y_axis)
plt.show()

	



