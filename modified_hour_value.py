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

time_scale="15 Min"
time_period='1705_1709'
name='*'

principal=500000.0
allFiles = glob.glob( time_period+"/*"+name+"*.csv")
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

all_trades['Date1']=new_date
all_trades['Date2']=new_date2

contract=[]

for name in allFiles:
	lhs,rhs=name.split("sp",1)
	lhs,rhs=rhs.split("_",1)
	contract.append(lhs[0:])
all_contract=set(contract)
all_contract=sorted(all_contract)


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
all_commodity=sorted(all_commodity)
	
	
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
duration=((end_date-start_date).days+1)*24*4

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
	duration=((end_date-start_date).days+1)*24*4
print start_date,end_date, ((end_date-start_date).days+1)*24*4


#dictionary of all price data
all_prices={}


for commodity in all_commodity:
		date1=toString(start_date)
		date2=toString(end_date)
		price_night=dd.getNightBars(commodity,date1,date2,time_scale)
		price_day=dd.getBars(commodity,date1,date2,time_scale)

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
value_to_date=np.zeros([len(all_commodity),duration])
pl=np.zeros([len(all_commodity),duration-1])
price_diff=np.zeros([len(all_commodity),duration-1])
amount_to_date=np.zeros([len(all_commodity),duration])
price_to_date=np.zeros([len(all_commodity),duration])
net_worth_to_date=np.zeros([len(all_commodity),duration])
if_trading=np.ones([len(all_commodity),duration])






amount_to_date[:,0]=0

base = start_date
date_list = [base + timedelta(hours=0.25)*x for x in range(0, duration)]



for index, row in all_trades.iterrows():
	commodity_name=get_name(index[0],date=True)
	commodity_index=list(all_commodity).index(commodity_name)
	amount_traded=row['Buy']*row['amount']*row['mult']
	time1=datetime(year=row['Date1'].year,month=row['Date1'].month,day=row['Date1'].day,hour=int(row['Time1'][0:3]),minute=int(row['Time1'][5:7]))
	time2=datetime(year=row['Date2'].year,month=row['Date2'].month,day=row['Date2'].day,hour=int(row['Time2'][0:3]),minute=int(row['Time2'][5:7]))
	if time1==time2:
		name_for_search=commodity_name[:-4]+commodity_name[-2:]
		expire_date=expiration[name_for_search]
		expire_date=datetime(year=row['Date2'].year,month=int(row['settle']/100.0),day=1)
		if int(toString(expire_date))>int(toString(end_date)):
			expire_date=end_date
		time2=expire_date
	time1_index=int((time1-start_date).seconds/3600.0*4.0+(time1-start_date).days*24.0*4.0)
	time2_index=int((time2-start_date).seconds/3600.0*4.0+(time2-start_date).days*24.0*4.0)
	amount_to_date[commodity_index,time1_index]+=amount_traded
	amount_to_date[commodity_index,(time1_index+1):(time2_index+1)]+=amount_traded


for commodity in all_commodity:
	commodity_index=list(all_commodity).index(commodity)
	price_data=all_prices[commodity]
	for index, row in price_data.iterrows():
		time=datetime(year=int(row['tradingday'][0:4]),month=int(row['tradingday'][4:6]),day=int(row['tradingday'][6:8]),hour=int(row['bartime'][0:2]),minute=int(row['bartime'][3:5]))
		time_index=(time-start_date).seconds/3600.0*4.0+(time-start_date).days*24.0*4.0
		price_to_date[commodity_index,int(time_index)]=row['closeprice']
	if_trading[commodity_index]=(price_to_date[commodity_index]!=0)
	
	
for vector in price_to_date:
	for i in range(duration):
		if vector[i]==0:
			vector[i]=vector[i-1]


	

		
price_diff=np.diff(price_to_date,axis=1)		



for i in range(len(price_diff[:,0])):
	for j in range(len(price_diff[0,:])):
		pl[i,j]=price_diff[i,j]*amount_to_date[i,j+1]

net_worth_to_date[:,0]=principal
		
for i in range(len(all_commodity)):
	for j in range(1,duration):
		net_worth_to_date[i,j]=net_worth_to_date[i,j-1]+pl[i,j-1]
		
net_worth_to_date*=if_trading
price_to_date*=if_trading
new_net_worth=[]
new_price=[]
		
for i in range(len(all_commodity)):
	new_net_worth.append([x for x in net_worth_to_date[i] if x!=0])
	new_price.append([x for x in price_to_date[i] if x!=0])



# for i in range(len(all_commodity)):
	# for j in range(duration):
		# if if_trading[i,j]==0:
			# date_list[j]=0
	
# date_list=[x for x in date_list if x!=0]

new_net_worth=np.asarray(new_net_worth)
# new_net_worth/=principal

new_price=np.array(new_price)


price_delta=[]
net_worth_sum=[]
for i in range(len(all_commodity)):
	if i % 2==1:
		price_delta.append(price_to_date[i-1]-price_to_date[i])
		net_worth_sum.append((net_worth_to_date[i-1]+net_worth_to_date[i])/2.0)





price_df={"date": date_list }
df = pd.DataFrame(data=price_df)
for i in range(len(all_contract)):
	df[all_contract[i]]=price_delta[i]
	df[all_contract[i]+'_worth']=net_worth_sum[i]/principal

df.to_csv('temp.csv')

# print pl
# print len(date_list) , duration
# pl_df={"date": date_list, "price_diff":price_delta }
# df = pd.DataFrame(data=pl_df)
# df.to_csv('temp.csv')

# net_worth=new_net_worth/principal
# net_worth_sum=np.sum(net_worth,axis=0)/len(all_commodity)
# fig=plt.figure()

# net_worth=fig.add_subplot(211)
# net_worth.plot(date_list,new_price[0])
# net_worth2=fig.add_subplot(212)
# net_worth2.plot(date_list,new_price[1])

# plt.show()


# n_plot=1
# fig=plt.figure()

# for i in range(n_plot):
	# price_plot=fig.add_subplot(n_plot,1,i+1)
	# price_plot.plot(date_list,net_worth_sum)


# plt.show()
	



