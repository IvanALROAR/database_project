import mysql.connector
import datetime
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf

def getPriceData(ticker, initial_date, period, db):
    ini_date = datetime.datetime.strptime(initial_date, "%d-%m-%Y")

    end_date = ini_date + datetime.timedelta(days=period)

    # query = 'SELECT {} FROM {} WHERE ticker=%s AND data_inici>=%s AND data_inici < %s'.format(columns, "preus") # string formatting method

    db.execute("SELECT * FROM preus " 
               "WHERE ticker=%s AND data_inici>=%s AND data_inici < %s", (ticker, ini_date, end_date))

    result = db.fetchall()
    df = pd.DataFrame(result, columns=('ticker', 'data_inici', 'data_final', 'open', 'high', 'low', 'close', 'volume'))
    df['data_inici'] = pd.to_datetime(df['data_inici'])  # set dates to datetime64
    df['data_inici'] = pd.to_datetime(df['data_final'])
    df.set_index('data_inici', inplace=True)
    return df

def getCountryData(country_code, initial_date, period, db, columns=None):
    ini_date = datetime.datetime.strptime(initial_date, "%d/%m/%Y")

    end_date = ini_date + datetime.timedelta(days=period)

    db.execute("SELECT * FROM dades_paisos WHERE"
               "id_pais=%s AND data_inici>=%s AND data_inici < %s", (country_code, ini_date, end_date))

    result = db.fetchall()
    df = pd.DataFrame(result, columns=('ticker', 'data_inici', 'data_final', 'open', 'high', 'low', 'close', 'volume'))
    df['data_inici'] = pd.to_datetime(df['data_inici'])  # set dates to datetime64
    df['data_inici'] = pd.to_datetime(df['data_final'])
    df.set_index('data_inici', inplace=True)

    return df

def getImportsData(pais_origen, pais_desti, initial_date, period, db, columns=None):
    ini_date = datetime.datetime.strptime(initial_date, "%d/%m/%Y")

    end_date = ini_date + datetime.timedelta(days=period)

    db.execute('SELECT * FROM imports WHERE id_pais_origen=%s AND id_pais_desti=%s AND data_inici>=%s AND data_inici < %s',
        (pais_origen, pais_desti, ini_date, end_date))

    result = db.fetchall()
    df = pd.DataFrame(result, columns=('ticker', 'data_inici', 'data_final', 'open', 'high', 'low', 'close', 'volume'))
    df['data_inici'] = pd.to_datetime(df['data_inici'])  # set dates to datetime64
    df['data_inici'] = pd.to_datetime(df['data_final'])
    df.set_index('data_inici', inplace=True)

    return df 





if __name__ == '__main__':
    password = ""
    cnx = mysql.connector.connect(user="root", password=password, database="mercatoanalisis")
    cursor = cnx.cursor()


    

    petroli = getPriceData("BZ=F", "1-01-2022", 1000, cursor)
    
    fusta = getPriceData("LBS=F", "1-01-2021", 1000, cursor)    
    
    # plot two series using matplotlib
    plt.plot(petroli.index, petroli["close"])
    plt.plot(fusta.index, fusta["close"])
    plt.xticks(rotation=60, ha='right')
    plt.show()

    # plot histogram using matplotlib hist
    plt.hist(petroli["close"], density=True, edgecolor='black', linewidth=1)
    plt.show()

    #mplfinance usage
    mpf.plot(petroli, type='candle', mav=(20, 50, 100), volume=True)

    # resample (e.g. daily --> weekly)
    petroli_weekly = petroli.copy()
    petroli_weekly = petroli_weekly.resample('1W').agg(
        {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
    )
    mpf.plot(petroli_weekly, type='candle', volume=True)

    cnx.commit()
    cursor.close()  