import pandas as pd
import os

for x in os.walk('extended_daily/'):
    if x[0][-6:] != 'daily/':
        try:
            print(os.listdir(x[0]))
            print(x[0])
            data = pd.DataFrame(columns=['Date', 'Price', 'Open', 'High', 'Low', 'Change %'])
            for i in os.listdir(x[0]):
                print(x[0] + '/' + i)
                df = pd.read_csv(x[0] + '/' + i)
                data = data.append(df)
            data['Close'] = data['Price']
            data = data[['Date', 'Open', 'High', 'Low', 'Close']]
            data['Open']=pd.to_numeric(data['Open'])
            data['High'] = pd.to_numeric(data['High'])
            data['Low'] = pd.to_numeric(data['Low'])
            data['Close'] = pd.to_numeric(data['Close'])
            data.info()
            data['Date'] = pd.to_datetime(data['Date'])
            data = data.sort_values(by='Date')
            data['Date'] = data['Date'].dt.strftime('%Y/%m/%d')
            data=data[data['Close']>0]
            print(data)
            data.to_csv('extended_D1/'+x[0][-6:]+'_daily.csv',index=False)
        except:
            continue
x = os.walk('extended_daily')
