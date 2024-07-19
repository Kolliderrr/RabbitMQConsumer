"""_summary_



"""
import dash_mantine_components as dmc
from dash_iconify import DashIconify
import dash_extensions as dext
import logging

from dash import Dash, _dash_renderer, html, dcc, Input, Output, State, callback_context, dash_table


_dash_renderer._set_react_version("18.2.0")

logger = logging.getLogger(__name__)
logger.setLevel(logging.ERROR)

def get_icon(icon):
    return DashIconify(icon=icon, height=16)

def create_dash(environ=None, start_response=None, server=None):
    if server:
        app = Dash(__name__, server=server, url_base_pathname='/dash_crossid_table/')
    else:
        app = Dash(__name__)
    
    style = {
        "border": f"1px solid {dmc.DEFAULT_THEME['colors']['indigo'][4]}",
        "textAlign": "center",
    }
    
    def generate_layout():
        pass
    
    main_layout = dmc.AppShell(
        [
            dmc.AppShellHeader(
                [
                    dmc.Grid(
                        children=[
                            dmc.GridCol(html.Div(id='badges-container', style=style), span=4),
                            dmc.Space(h=5),
                            dmc.GridCol(html.Div(id='paper-container', style=style), span=4),
                            dmc.Space(h=5),
                            dmc.GridCol(html.Div(style=style), span=4),
                        ],
                        gutter="s",
                    )
                ]
                ),
            dmc.AppShellMain([
                html.Div(
                    [
                        html.Div(
                            [
                                dmc.Button("Меню", id="drawer-button"),
                                dmc.Drawer(
                                    title="Меню",
                                    id="drawer-container",
                                    padding="md",
                                    zIndex=10000,
                                    children=[
                                        html.Div([
                                            dmc.NavLink(
                                            label="Домой",
                                            leftSection=get_icon(icon="bi:house-door-fill"),
                                            href='http://192.168.20.122:8081/',
                                            target='_blank'
                                        ),
                                            dmc.NavLink(
                                            label="Потребители",
                                            description="Управление потребителями",
                                            leftSection=dmc.Badge(
                                                "0", size="xs", variant="filled", color="green", w=16, h=16, p=0
                                            ),
                                        ),
                                        dmc.NavLink(
                                            label="Соединения",
                                            leftSection=get_icon(icon="tabler:gauge"),
                                            rightSection=get_icon(icon="tabler-chevron-right"),
                                            children=[
                                                dmc.NavLink(label="Брокеры (серверы передачи данных)"),
                                                dmc.Space(h=5),
                                                dmc.NavLink(label="Базы данных"),
                                            ]
                                        ),
                                        
                                        dmc.NavLink(
                                            label="Настройки приложения",
                                            leftSection=get_icon(icon="tabler:activity"),
                                            rightSection=get_icon(icon="tabler-chevron-right"),
                                            variant="subtle",
                                            active=True,
                                            children=[
                                                dmc.NavLink(label="Общие"),
                                                dmc.Space(h=5),
                                                dmc.NavLink(label="Настройки обновления"),
                                                dmc.Space(h=5),
                                                dmc.NavLink(label="Помощь")
                                            ]
                                        )
                                        ],style={"width": 240, "align-top": 'center'},)
                                    ]
                                ),
                            ]
                        ),
                        html.Div(id='table-headers-container'),
                        html.Div(id='table-container'),
                        html.Div(id='actions-container')
                    ]
                )
            ]),
        ],
        header={"height": 90})
    
    app.layout = dmc.MantineProvider([main_layout])

    @app.callback(
    Output("drawer-container", "opened"),
    Input("drawer-button", "n_clicks"),
    prevent_initial_call=True,
    )
    def drawer_demo(n_clicks):
        return True
    
    return app
if __name__ == '__main__':
    app = create_dash()
    app.run(debug=True, port=8787, host='127.0.0.1')