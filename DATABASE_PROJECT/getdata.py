import yfinance as yf
import pandas as pd

# llista de tickers a consultar
tickers = ["GC=F", "SI=F", "PL=F", "HG=F", "PA=F","CL=F","HO=F","NG=F","RB=F","BZ=F",
            "B0=F","ZC=F","ZO=F","KE=F","ZR=F","ZM=F","ZL=F","ZS=F","CC=F",
            "KC=F","CT=F","OJ=F","SB=F","LBS=F","HE=F","LE=F","GF=F"]

data_list = list()
for ticker in tickers:
    data = yf.download(ticker, interval="1d", period="100y")
    data["Ticker"] = ticker
    data_list.append(data)

df = pd.concat(data_list)

df = df.reset_index()

header = ["Date", "Ticker", "Open", "High", "Low", "Close", "Volume"]

df.to_csv("./data.csv", columns = header, index=False)