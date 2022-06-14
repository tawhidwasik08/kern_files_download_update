import pandas as pd
import zipfile
import os
from alive_progress import alive_bar
import time
from pathlib import Path


parent_dir = Path(__file__).resolve().parent
origin_dir = Path(parent_dir).resolve().parent

jai_kern_download_dir = os.path.join(origin_dir, "Downloads/jai.kern/")
stp_kern_download_dir = os.path.join(origin_dir, "Downloads/stp.kern.user/")

jai_kern_save_dir = os.path.join(origin_dir, "Datasets/")
stp_kern_save_dir = os.path.join(origin_dir, "Datasets/")


KEEP_DEVICE_CODE_DIC = {
    "351358816433780": "CU_TI_300W",
    "351358816432345": "CU_TI_301W",
    "351358816432626": "CU_TI_303W",
    "351358816438466": "CU_PI_404W",
    "351358816438219": "CU_TI_304W",
    "351358816436361": "CU_PI_405W",
    "351358816432113": "CU_TI_306W",
    "351358816436734": "CU_PI_406W",
    "351358816432204": "CU_TI_308W",
    "351358816435132": "CU_TI_317W",
    "351358816436221": "CU_PI_407W",
    "351358816432394": "CU_TI_318W",
    "351358816432022": "CU_TI_319W",
    "351358816435504": "CU_PI_409W",
    "351358816432568": "CU_TI_320W",
    "351358816433830": "CU_TI_321W",
    "351358816436957": "CU_PI_411AW",
    "351358816436627": "CU_PI_411BW",
    "351358816434838": "KHT_TI_300W",
    "351358816433913": "KHT_TI_301W"
}
	


def load_dataframe_from_file(root_dir, file_ext, file_type):

	df_list = []

	file_name_list = os.listdir(root_dir)
	total_file_count = len(file_name_list)

	with alive_bar(total_file_count, title=f'Files: {file_type}') as bar:
		for i in range(total_file_count):
			filename = file_name_list[i]
			if filename.endswith(file_ext):
				dataset_path = root_dir+filename
				if file_ext == ".zip":
					zf = zipfile.ZipFile(dataset_path)
					df_list.append(pd.read_csv(zf.open('dashboard-iview/all_devices_with_latest_sensor_data.csv')).iloc[:,1:])
				else:
					df_list.append(pd.read_csv(dataset_path, ))
			time.sleep(.005)
			bar()


	df = pd.concat(df_list)

	for single_df in df_list:
		del single_df 
	del df_list      

	return df


def clean_merge_new_sensor_data(load_dir, save_dir):
	
	try:
		df = load_dataframe_from_file(load_dir, ".zip", "New Sensor Data")
	except ValueError:
		print("Error: Please make sure atleast one file is in the Downloads/jai.kern folder.")
		return


	df.drop_duplicates(keep="first", inplace=True)

	df = df[df['Device Code'].map(lambda x: str(x) in KEEP_DEVICE_CODE_DIC.keys())]
	df = df[df['Unit of Measurement'].map(lambda x: str(x) in ["F", "psig", "psi"])]
	df = df.sort_values("Timestamp Time", ascending=False)
	df['Tag Name'] = df[['Device Code']].apply(lambda x: KEEP_DEVICE_CODE_DIC[x["Device Code"]], axis=1)
	df.to_csv(save_dir+"New_Sensors.csv", sep=",", encoding="UTF-8", index=False)

	return None


def merge_old_sensor_data(load_dir, save_dir):
	
	try:
		df = load_dataframe_from_file(load_dir, ".csv", "Old Sensor Data")
	except ValueError:
		print("Error: Please make sure atleast one file is in the Downloads/stp.kern.user folder.")
		return
	df.drop_duplicates(keep="first", inplace=True)	
	df = df.sort_values("Date", ascending=False)
	df.to_csv(save_dir+"Old_Sensors.csv", sep=",", encoding="UTF-8", index=False)

	return None


if __name__ == "__main__":
    
    print("\nUpdate starting for existing files....\n")

    clean_merge_new_sensor_data(jai_kern_download_dir, jai_kern_save_dir)
    merge_old_sensor_data(stp_kern_download_dir, stp_kern_save_dir)

    print("Datasets are updated.\n\n")
