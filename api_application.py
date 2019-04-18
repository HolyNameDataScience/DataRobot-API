import pandas as pd
import datarobot as dr

__update__ = '12/18'


# this class is used to manipulate input
class DataPrepare(object):
    @staticmethod
    def replace_variable_names(original_feature_list, old_features, new_features):
        for i in range(len(old_features)):
            feature = old_features[i]
            temp_index_list = [i for i, val in enumerate(original_feature_list) if val == feature]
            for j in temp_index_list:
                original_feature_list[j] = new_features[i]
        return original_feature_list

    @staticmethod
    def map_df_names(df, mapping_list):
        for i in range(len(mapping_list)):
            temp_df = df.rename(columns={mapping_list[i][0]: mapping_list[i][1]})
        return temp_df


class DataRobotAPI(object):
    def __init__(self, api_token, endpoint):
        self.api_token = api_token
        self.endpoint = endpoint
        dr.Client(endpoint=self.endpoint, token=self.api_token)
        self.my_projects = None
        self.project = None
        self.target = None

    def define_target(self, target):
        self.target = target

    def modify_feature_data_type(self, feature_type_change_list):
        raw = [feat_list for feat_list in self.project.get_featurelists() if feat_list.name == 'Raw Features'][0]
        print("raw")
        print(raw)
        for j in range(len(feature_type_change_list)):
            before_change = feature_type_change_list[j][0]
            after_change = feature_type_change_list[j][1]
            self.project.create_type_transform_feature(
                "peudo_id(Cat)",  # new feature name
                "peudo_id",  # parent name
                dr.enums.VARIABLE_TYPE_TRANSFORM.CATEGORICAL_INT)
            temp_feature_list = list((set(raw.features) - {before_change}) | {after_change})
        feature_list_name = "new_feature_list"
        new_feature_list = self.project.create_featurelist(feature_list_name, temp_feature_list)
        self.project.start_autopilot(new_feature_list.id)
        return new_feature_list

    def run_autopilot(self, feature_list):
        self.project.start_autopilot(feature_list.id)

    def get_project_list(self):
        self.project_list = dr.Project.list()
        return self.project_list

    def get_project_name_list(self):
        project_list = self.get_project_list()
        self.project_name_list = [x.project_name for x in project_list]
        return self.project_name_list

    def remove_a_project(self, project_name):
        curr_project_name_list = self.get_project_name_list()
        if project_name in curr_project_name_list:
            current_project = dr.Project.list(search_params={'project_name': project_name})[0]
            current_project.delete()

    def get_project_list(self):
        self.my_projects = dr.Project.list()
        print("current projects # is :" + str(len(self.my_projects)))

    def create_a_project(self, project_name, target_name, input_df):
        self.my_projects = dr.Project.list()
        self.target = target_name
        my_project_names = [x.project_name for x in self.my_projects]
        print(my_project_names)

        while project_name in my_project_names:
                print("already exist a project have the same name")
                print(dr.Project.list(search_params={'project_name': project_name}))
                temp_project = dr.Project.list(search_params={'project_name': project_name})[0]
                temp_project.delete()
                my_project_names.remove(my_project_names[my_project_names.index(project_name)])
        self.project = dr.Project.start(input_df, project_name=project_name, target=target_name)

    def define_features(self, project, feature_list_name, feature_list_id):
        featurelist = project.create_featurelist(feature_list_id, feature_list_name)
        return featurelist


if __name__ == "__main__":
    API_TOKEN = 'M3OAYZpaxpzzJncUHTX7UZ4qThY-byIq'
    endpoint_url = 'http://34.206.143.40/api/v2'
    DR = DataRobotAPI(API_TOKEN, endpoint_url)
    input_dir = r"H:\Public\ACO\Analytics\Ma,X\PredictiveModels\PythonCode\FinalCode" \
                r"\RisingRisk\RisingRisk5.0\temp_folder\test2.csv"
    data_df = pd.read_csv(input_dir)
    original_feature_list = ['NextRateOfChange', 'Date', 'PaymentDate', 'DateValue'
        ,'EMA(2)', 'EMA(5)', 'EMA(10)', 'EMA(15)', 'EMA(20)', 'EMA(30)'
        ,'EMA(50)', 'MACD Line', 'EMA(9) MACD Line', 'MACD Histogram'
        ,'RateOfChange',  'MostRecentPrimDiag', 'PrimDiagCode', 'NumPrimDiag'
        ,'BeneficiaryDateofBirth', 'BeneficiaryZIP_Code', 'BeneficiarySexCode'
        ,'BeneficiaryRaceCode', 'new_id', 'curr_age', 'peudo_id']

    input_df = data_df[original_feature_list].copy()
    feature_name_mapping_list = [['BeneficiaryZIP_Code', 'z_var']]
    input_df = DataPrepare().map_df_names(input_df, feature_name_mapping_list)
    DR.create_a_project("RR5.0_target_rate_of_change", "NextRateOfChange", input_df)
    feature_type_change_list = [["peudo_id(Cat)",  "peudo_id"]]
    fl = DR.modify_feature_data_type(feature_type_change_list)
    DR.run_autopilot(fl)


