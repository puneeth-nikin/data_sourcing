import pandas as pd
import os


def extended(symbol):
    data = pd.DataFrame(columns=['Date', 'Price', 'Open', 'High', 'Low', 'Change %'])
    for i in os.listdir(symbol):
        print(symbol + '/' + i)
        df = pd.read_csv(symbol + '/' + i)
        data = data.append(df)
    data['Close'] = data['Price']
    data = data[['Date', 'Open', 'High', 'Low', 'Close']]
    data['Open'] = pd.to_numeric(data['Open'])
    data['High'] = pd.to_numeric(data['High'])
    data['Low'] = pd.to_numeric(data['Low'])
    data['Close'] = pd.to_numeric(data['Close'])
    data.info()
    data['Date'] = pd.to_datetime(data['Date'])
    data = data.sort_values(by='Date')
    data['Date'] = data['Date'].dt.strftime('%Y/%m/%d')
    data = data[data['Close'] > 0]
    print(data)
    data = data.drop_duplicates()
    data.to_csv('extended_D1/' + symbol[-6:] + '_daily.csv', index=False)

extended('extended_daily/USDSGD')

# for x in os.walk('extended_daily/'):
#     print(x[1])
#     if x[0][-6:] != 'daily/':
#         try:
#             print(os.listdir(x[0]))
#             print(x[0])
#             extended(x[0])
#
#         except:
#             continue
# z=None
# for x in os.walk('extended_D1'):
#     z=x[2]
#     print(x[2])
# print(z)
# for x in z:
#     if x!='.DS_Store':
#         print(x)
#         data=pd.read_csv('extended_D1/'+x)
#         print(len(data))
#         data=data.drop_duplicates()
#         print(len(data))
#         data.to_csv('extended_D1/' +x, index=False)
