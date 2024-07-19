import dash_mantine_components as dmc
from dash import html
import pandas as pd
from typing import Union, Dict, List, Any, Tuple


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
                for element in content[1:]
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

def generate_table_headers(columns: Union[List[str], tuple]) -> dmc.TableThead:
    """Генератор заголовков таблицы dmc.Table

    Args:
        columns (Union[List[str], tuple]): Входящее значение [List] или [Tuple]
 
    Returns:
        dmc.TableThead: заголовки для dash_mantine_components.Table
    """
    return dmc.TableThead(
        dmc.TableTr(
                [
                    dmc.TableTh(key)
                    for key in columns
                ]
            )
        )

def generate_table_row(data: Union[Dict[List[Any]], Dict[Tuple[Any]]]) -> dmc.TableTr[dmc.TableTd]:
    """_summary_

    Args:
        data (Union[Dict[List[Any]], Dict[Tuple[Any]]]): на входе - словарь с названием потребителя в ключе
            и Union[List, Tuple] данных

    Returns:
        dmc.TableTr[dmc.TableTd]: Body таблицы
    """
    return [
            dmc.TableTr(
                [
                    dmc.TableTd(el, id=f'{key}-{i}-cell')
                    for i, el in elements
                ],
                id=f'{key}-row'
            )
        for key, elements in data.items()
            ]