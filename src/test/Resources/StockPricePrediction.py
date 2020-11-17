
import sys
import pandas as pd
import pickle


# Taking Input Through Standard Input
inputStockPriceFile = sys.stdin
# Creating dataframe from standard input
inputStockPriceDF = pd.read_csv(inputStockPriceFile, header=None)
# Providing name to the columns
inputStockPriceDF.rename(columns={0: "Open", 1: "High", 2: "Low", 3: "Volume"}, inplace=True)
# Cleanising the Volume and Open Columns
inputStockPriceDF["Volume"] = inputStockPriceDF["Volume"].apply(lambda x: x.replace("]", ""))
inputStockPriceDF["Open"] = inputStockPriceDF["Open"].apply(lambda x: x.replace("[", ""))
# Loading the dumped LinearRegressionModel pickle file
with open("./src/test/Resources/StockPriceModel.pkl", "rb") as modelFile:
    linearRegressionModel = pickle.load(modelFile)

# Predicting the stock close price
predictedClosePrice = linearRegressionModel.predict(inputStockPriceDF)
# Printing  the output through standard output
for closePrice in predictedClosePrice:
    print(round(closePrice, 2))