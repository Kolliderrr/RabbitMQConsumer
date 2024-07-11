import dash_mantine_components as dmc
from dash import html
import pandas as pd
from typing import Union, Dict, List, Any


class MyException(Exception):
    pass

def generate_table(content: Union[List[Dict[str, Any]], pd.DataFrame]) -> dmc.Table:
    try:
        if isinstance(content, pd.DataFrame):
            elements = content.to_dict('records')
            rows = [
                    dmc.TableTr(
                        [
                            dmc.TableTd(element[key])
                            for key in element.keys()    
                        ]
                    )
                for element in elements
            ]
            
            header = dmc.TableThead(
            dmc.TableTr(
                    [
                        dmc.TableTh(key)
                        for key in content.columns.to_list()
                    ]
                )
            )
            
            body = dmc.TableTbody(rows)
            return dmc.Table([header, body])
        elif isinstance(content, list):
            rows = [
                    dmc.TableTr(
                        [
                            dmc.TableTd(element[key])
                            for key in element.keys()    
                        ]
                    )
                for element in content
            ]
            
            header = dmc.TableThead(
            dmc.TableTr(
                    [
                        dmc.TableTh(key)
                        for key in content[0]
                    ]
                )
            )
            
            body = dmc.TableTbody(rows)
            return dmc.Table([header, body])
        else:
            raise MyException('Неправильный формат данных для таблицы! См. описание')
    except MyException:
        return html.Div('Неправильный формат данных')
        

