from dotenv import load_dotenv
from backtest.minervini import *

###################### MAIN ######################
if __name__ == "__main__":
    # Main variables
    load_dotenv()
    DIR_DATA = str(os.getenv('DIR_DATA'))
    DIR_SUB_DATA = str(os.getenv('DIR_SUB_DATA'))

    # Get all csvs under each folder
    data_folders = os.listdir(DIR_DATA, DIR_SUB_DATA)
    for folder in data_folders:
        raw_csvs = os.listdir(os.path.join(DIR_DATA, DIR_SUB_DATA, folder))
        for raw_csv in raw_csvs:
            raw_csvdir = os.path.join(DIR_DATA, DIR_SUB_DATA, folder, raw_csv)
            symbol = raw_csv.split('.')[0]

            # Read csv into dataframe
            df = pd.read_csv(raw_csvdir)
            latestdf = dfSetupMinervini(df)
            o=0
