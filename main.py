import pandas as pd
import os


class DataLoader():
    """
    Class which is used for loading data from files into pandas dataframes
    """

    print("Pobranie danych")

    def __init__(self, nwpDataPath):
        """
        :param nwpDataPath: path to folder where monthly catalogs with forecast data are available
        """
        self.nwpDataPath = nwpDataPath

    def divideFiles(self):
        """
        Function which divide files paths into three categories - solar radiation forecast files, wind forecast files
        and temperature forecast files
        :return: sr, temp, ws - lists with files names for given data category
        """

        # Empty lists for files paths
        sr = []
        temp = []
        ws = []

        # Division of files according to data category
        for root, dirs, files in os.walk(self.nwpDataPath):
            for file in files:
                if 'sr' in file:
                    sr.append(file)
                elif 'ws' in file:
                    ws.append(file)
                elif 'temp' in file:
                    temp.append(file)

        return sr, temp, ws

    def loadNwpData(self):
        """
        Function which reads data from previosly divided into category files and creates dataframes with data for given
        parameter
        :return: dfSr, dfTemp, dfWs -  dataframes with forecast data
        """

        # Empty dataframes for data from given category
        dfSr = []
        dfTemp = []
        dfWs = []

        # File paths divided into categories
        sr, temp, ws = self.divideFiles()

        # Loading data from files into specific dataframes
        for root, dirs, files in os.walk(self.nwpDataPath):

            # Value 6 below means that we are trying to get values from
            # C:\Users\a.sochaj\KPEC_REKONFIGURACJA_PROGNOZY_OBCIAZENIA\InputHome\nwp  not from
            # C:\Users\a.sochaj\KPEC_REKONFIGURACJA_PROGNOZY_OBCIAZENIA\InputHome\nwp\201901 which has 7 element after
            # splitting by \

            if len(root.split("\\")) == 6:
                for file in sr:
                    if file.endswith('.txt'):
                        dir = file.split("_")[2][:6]
                        file_path = os.path.join(root, dir, file)

                        dfRaw = (pd.read_csv(file_path)
                                 .assign(
                            Area=file.split("_")[0],
                            Parameter="sr",
                            Data=file.split("_")[2],
                            PTimePredUTC=lambda df_: pd.to_datetime(df_.PTimePredUTC, #format='%Y-%m-%d %H:%M:%S'
                                                                     ),
                            TimePredUTC=lambda df_: pd.to_datetime(df_.TimePredUTC, #format='%Y-%m-%d %H:%M:%S'
                                                                    )
                        )
                                 .drop_duplicates()
                                 )

                        dfSr.append(dfRaw)

                for file in temp:
                    if file.endswith('.txt'):
                        dir = file.split("_")[2][:6]
                        file_path = os.path.join(root, dir, file)

                        dfRaw = (pd.read_csv(file_path)
                                 .assign(
                            Area=file.split("_")[0],
                            Parameter="temp",
                            Data=file.split("_")[2],
                            PTimePredUTC=lambda df_: pd.to_datetime(df_.PTimePredUTC, #format='%Y-%m-%d %H:%M:%S'
                                                                     ),
                            TimePredUTC=lambda df_: pd.to_datetime(df_.TimePredUTC, #format='%Y-%m-%d %H:%M:%S'
                                                                    )
                        )
                                 .drop_duplicates()
                                 )

                        dfTemp.append(dfRaw)

                for file in ws:
                    if file.endswith('.txt'):
                        dir = file.split("_")[2][:6]
                        file_path = os.path.join(root, dir, file)

                        dfRaw = (pd.read_csv(file_path)
                                 .assign(
                            Area=file.split("_")[0],
                            Parameter="ws",
                            Data=file.split("_")[2],
                            PTimePredUTC=lambda df_: pd.to_datetime(df_.PTimePredUTC, #format='%Y-%m-%d %H:%M:%S'
                                                                     ),
                            TimePredUTC=lambda df_: pd.to_datetime(df_.TimePredUTC, #format='%Y-%m-%d %H:%M:%S'
                                                                    )
                        )
                                 .drop_duplicates()
                                 )
                        dfWs.append(dfRaw)

        # Concatenation of loaded dataframes into one, general dataframe per parameter
        dfSr = pd.concat(dfSr, ignore_index=True)
        dfTemp = pd.concat(dfTemp, ignore_index=True)
        dfWs = pd.concat(dfWs, ignore_index=True)

        return dfSr, dfTemp, dfWs


class DataTransformer():
    """
    A class used to transform data to the desired structure and horizon length, forecasts ending with writing data
    to txt files in a specified location.
    """

    def __init__(self, dfSr, dfTemp, dfWs, destinationPath, targetHorizon):
        """
        Function used for class inicialization using provided data.
        :param dfSr: dataframe with solar radiation data,
        :param dfTemp: dataframe with ambient temperature data,
        :param dfWs: dataframe with wind speed data,
        :param destinationPath: path to do folder where data should be saved
        :param targetHorizon: desired forecast horizon in hours.
        """
        print("Obróbka i zapis danych")
        self.dataFrames = [dfSr, dfTemp, dfWs]
        self.targetHorizon = targetHorizon
        self.destinationPath = destinationPath
        self.lenghtList = []

    def combineData(self, dfDay, updateTime, dfArea, day):
        currentUpdateTimeDataframe = (dfDay
                                      .loc[(dfDay.PTimePredUTC == updateTime)]
                                      .reset_index(drop=True)
                                      .drop_duplicates(subset=['TimePredUTC'])
                                      )

        nextUpdateTimeDataframe = (dfArea
                                       .loc[lambda df_: (df_['PTimePredUTC'] == updateTime
                                                         + pd.Timedelta(
                    hours=24))  # 24 because some forecast 12h ahead have no full data
                                                        &
                                                        (df_['TimePredUTC'] > (
                                                        currentUpdateTimeDataframe['TimePredUTC']).max())
                                   ]
                                       .reset_index(drop=True)
                                       .drop_duplicates(subset=['TimePredUTC'])
                                       .iloc[:(60 - len(currentUpdateTimeDataframe)), :]
                                       )

        return (pd.concat([currentUpdateTimeDataframe, nextUpdateTimeDataframe], ignore_index=True)
            .assign(
            PTimePredUTC=updateTime,
            Data=day
        )
        )

    def saveFile(self, day, area, updateTimeDataframe, dayDataframes):
        # Creation of path for saving file
        filePath = os.path.join(self.destinationPath,
                                day[:-2],
                                "_".join([area, updateTimeDataframe.Parameter.values[0], day, 'nwp.txt']))

        # Creation of given directory if it does't exist
        os.makedirs(os.path.dirname(filePath), exist_ok=True)

        # Concatenation of daily data and saving into desired file path
        (pd.concat(dayDataframes, ignore_index=True)
         .drop(['Area', 'Parameter', 'Data'], axis=1)
         .to_csv(filePath, sep=',', index=False))

    def transform(self):
        """
        Function used for transforming data into desired forecast horizon and saving data into txt files.
        :return: data saved in destinationPath folder.
        """
        # Iteration through provided dataframes
        for dataframe in self.dataFrames:

            # List with all unique area values available in data
            areas = dataframe['Area'].unique()

            # Iteration through unique areas
            for area in areas:

                # Dataframe with data for a specific area from source data
                dfArea = dataframe.loc[dataframe.Area == area]

                # List with all unique days values available in data
                days = dfArea['Data'].unique()

                # Iteration through unique days
                for day in days:

                    # Dataframe with data for a specific area and day from source data
                    dfDay = dfArea.loc[dataframe.Data == day]
                    dayDataframes = []

                    # List with all unique dates of forecast generation available in data
                    updateTimes = dfDay["PTimePredUTC"].unique()

                    # Iteration through unique dates of forecast generation
                    for updateTime in updateTimes:

                        # Data transformation for dataframes without duplicastes in TimePredUTC column
                        try:
                            updateTimeDataframe = self.combineData(dfDay, updateTime, dfArea, day)


                        # Data transformation for dataframes with duplicastes in TimePredUTC column
                        except ValueError:
                            print(f'{ValueError=}')

                        # Printing of updateTime, day, area for which possible error occured
                        except Exception as e:
                            print(e)
                            print(updateTime, day, area, )

                        # Loading daily data into one dataframe
                        dayDataframes.append(updateTimeDataframe)

                    # Save files
                    self.saveFile(day, area, updateTimeDataframe, dayDataframes)


def main():
    # Path to source folder with nwp data
    nwpDataPath = r"C:\Users\a.sochaj\KPEC_REKONFIGURACJA_PROGNOZY_OBCIAZENIA\InputHome\nwp"

    # Path to folder where transformed, with desired forecast horizon data will be saved
    destinationPath = r'C:\Users\a.sochaj\KPEC_REKONFIGURACJA_PROGNOZY_OBCIAZENIA\InputHome\nwp_new'

    # Desired forecast horizon
    targetHorizon = 60

    # Creation of DataLoader class instance
    dataLoaderObj = DataLoader(nwpDataPath)

    # Creation of dataframes with data for a specific parameter
    dfSr, dfTemp, dfWs = dataLoaderObj.loadNwpData()

    # Creation of DataTransformer class instance
    dataTransforObj = DataTransformer(dfSr, dfTemp, dfWs, destinationPath, targetHorizon)

    # Transformation and saving data into desired path
    dataTransforObj.transform()

    print("Działanie zakończone")



if __name__ == "__main__":
    main()





