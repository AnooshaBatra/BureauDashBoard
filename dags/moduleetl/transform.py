
import pandas as pd
import json


def transform_data(ti):
    data_json = ti.xcom_pull(key='api_data', task_ids='get_data_from_api')
    data_frame=pd.DataFrame()
    data_dict= json.loads(data_json)
    for i in range(len(data_dict)):
        if 'hits' in data_dict[i]:
            for doc in  data_dict[i]['hits']['hits']:
                    doc_updated = doc['_source']
                    #print("doc_updated type is",type(doc_updated))
                    data_frame = data_frame.append(doc_updated, ignore_index=True)


    print("orginal size is",data_frame.size)
    updated_df= data_frame.drop(['complaint_what_happened', 'date_sent_to_company','zip_code', 'tags','has_narrative','consumer_consent_provided','consumer_disputed','company_public_response'], axis = 1)
    updated_df['MonthYear'] = pd.to_datetime(updated_df['date_received'], format='%Y-%m-%d')
    updated_df['MonthYear'] = updated_df['MonthYear'].dt.strftime('%Y-%m-30')
    updated_df = updated_df.fillna(0)
    transformed_df=updated_df.groupby(['product','sub_product','issue','sub_issue','timely','submitted_via','company','company_response','state','MonthYear']).agg({"complaint_id": pd.Series.nunique})
    transformed_df.to_csv('transform.csv')

    return 'sucess'