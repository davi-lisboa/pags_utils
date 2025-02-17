# Bibliotecas
import pandas as pd
# import polars as pl
# import numpy as np
# import datetime as dt

# import requests
# import wget
# from bs4 import BeautifulSoup

# import statsmodels.api as sm

# from sklearn.linear_model import LinearRegression, Lasso, Ridge
# from sklearn.model_selection import train_test_split
# from sklearn.preprocessing import LabelEncoder, StandardScaler, MinMaxScaler
# from sklearn.metrics import mean_squared_error as mse, r2_score, root_mean_squared_error as rmse

# import pmdarima as pmd


# import matplotlib.pyplot as plt
# import seaborn as sns
# import plotnine as p9

# import os


# get_abecs
def get_abecs(card_type = ['Crédito', 'Débito', 'Pré-Pago', 'Total'] ):
    import pandas as pd
    import requests
    import wget
    import os
    from bs4 import BeautifulSoup
    
    import warnings
    warnings.filterwarnings('ignore')

    url_base = "https://www.abecs.org.br/graficos"
    req = requests.get(url_base, verify=False)
    soup = BeautifulSoup(req.content, 'html.parser')

    link_download = soup.find(class_="btn btn-secondary btn-icon btn-icon-left my-3 my-lg-0").get("href")

    arq_name = 'abecs_tpv.xlsx'
    wget.download(link_download, arq_name)

    if type(card_type) == list:
        abecs = pd.read_excel(arq_name,  skiprows=9, usecols=['Período'] + card_type).dropna()
        
    elif type(card_type) == str:
        card_type = card_type.capitalize()
        abecs = pd.read_excel(arq_name,  skiprows=9, usecols=['Período'] + [card_type]).dropna()

    abecs = abecs.rename(columns={'Período': 'date'})
    abecs.set_index('date', inplace=True)
    abecs.index = pd.to_datetime(abecs.index)

    os.remove(arq_name)

    return abecs



# get_mdic
def get_mdic():
    import pandas as pd
    import requests
    import wget
    import os
    from bs4 import BeautifulSoup
    
    import warnings
    warnings.filterwarnings('ignore')

    url_base = "https://balanca.economia.gov.br/balanca/publicacoes_dados_consolidados/pg.html#totais"
    req = requests.get(url_base)
    soup = BeautifulSoup(req.content, 'html.parser')

    link_download = "https://balanca.economia.gov.br/balanca" 
    link_download += soup.find(class_="btn btn-default btn-xs bt-dwn").get("href").replace('..', '')

    arq_name = 'mdic_totais.xlsx'
    wget.download(link_download, arq_name)

    mdic = pd.read_excel(arq_name, skipfooter=1, skiprows=5, usecols=['Data', 'Exportações', 'Importações', 'Saldo', 'Corrente'])
    mdic.Data = pd.to_datetime(mdic.Data, format='%m/%Y')
    mdic.sort_values(by='Data', inplace=True)
    mdic.set_index('Data', inplace = True)
    os.remove(arq_name)

    return mdic


# get_stn
def get_stn() -> pd.DataFrame:
    import pandas as pd
    import numpy as np
    import datetime as dt
    
    import requests
    import wget
    
    import os



    url = 'http://www.tesourotransparente.gov.br/ckan/api/3/action/package_show?id=resultado-do-tesouro-nacional'
    req = requests.get(url)
    
    # req
    df_link = req.json()['result']['resources'][0]['url']

    arq_name = wget.detect_filename(df_link)
    wget.download(df_link, arq_name)

    df = (pd.read_excel(
                        arq_name, 
                        sheet_name='1.1', 
                        skiprows=4, 
                        skipfooter=10
                       )
          .set_index('Discriminação')
          .transpose()
         )

    df.index.name = ''
    df.columns.name = ''
    df.index = pd.to_datetime(df.index)

    res = []
    for col in df.columns:
        if 'RESULTADO' in col:
            res.append(col)

    df = df[res]

    os.remove(arq_name)

    
    return df



# fmt_nixtla
# def fmt_nixtla(df, date_col_name = 'ds', vars_col_name = 'unique_id', value_name = 'y', show_var_names = False, label_encode = False):
#     import pandas as pd
#     import datetime as dt
#     from sklearn.preprocessing import LabelEncoder

#     if type(df.index) != pd.core.indexes.datetimes.DatetimeIndex:
#         raise TypeError("DataFrame Index must be datetime-like")


#     fmt_df = df.copy()
#     fmt_df.index.name = date_col_name

#     fmt_df = pd.melt(frame = fmt_df.reset_index(),
#                     id_vars = date_col_name,
#                     var_name = vars_col_name,
#                     value_name = value_name)

#     if label_encode == True:
#         label_encoder = LabelEncoder()
#         fmt_df[vars_col_name] = label_encoder.fit_transform(fmt_df[vars_col_name])

#     if show_var_names == True:
#         fmt_df['id_names'] = label_encoder.inverse_transform(fmt_df[vars_col_name])

#     return fmt_df


# def df_metrics(df, metrics = ['mse', 'rmse', 'mae', 'mape', 'smape'], models: list = None):
#     from utilsforecast.losses import mse, rmse, mae, mape, smape
#     import pandas as pd

#     mse_ = mse(df = df, models = models)
#     rmse_ = rmse(df = df, models = models)
#     mae_ = mae(df = df, models = models)
#     mape_ = mape(df = df, models = models)
#     smape_ = smape(df = df, models = models)

#     metrics_list = [mse_, rmse_, mae_, mape_, smape_]

#     df_metrics = pd.concat(metrics_list).round(4)
#     df_metrics.index = metrics

#     return df_metrics

def get_stone(
                start: None | str = None, 
                end:None | str = None,
                date_index = True,
                uf_pais: str = 'all',
                regiao: str = 'all',
                setor: str = 'all',
                tipo: str = 'all'
            ) -> pd.DataFrame:
    """
        Recupera os dados do Índice Stone de Varejo e retorna um DataFrame do Pandas.
    
        Args:
            start: Data inicial para o filtro (formato: 'YYYY-MM-DD').
            end: Data final para o filtro (formato: 'YYYY-MM-DD').
            date_index: Se True, a coluna 'mes' será o índice do DataFrame.
            uf_pais: UF/país para filtrar. Use 'all' para incluir todos.
            regiao: Região para filtrar. Use 'all' para incluir todas.
            setor: Setor de atividade para filtrar. Use 'all' para incluir todos.
            tipo: Tipo para filtrar. Use 'all' para incluir todos.
    
        Returns:
            pd.DataFrame: DataFrame com os dados do Índice Stone, filtrados e formatados.
    """
    import pandas as pd
    import requests
    from bs4 import BeautifulSoup
    from io import BytesIO

    # 'https://conteudo.stone.com.br/wp-content/uploads/2025/01/Indice-do-varejo-Stone-dezembro-2024.csv'
    url = "https://conteudo.stone.com.br/indice-do-varejo-stone/"
    req = requests.get(url)
    
    if req.status_code == 200:
        print('Requisição concluída')
    else:
        print('Houve algum erro ao executar a requisição')

    html = req.content
    soup = BeautifulSoup(html, 'html.parser')

    for link in soup.find_all('a'):
        url = link.get('href')
        if type(url) == str:
            if '.csv' in url:
                stone_link = url
                break   
    req_stone = requests.get(stone_link)
    stone_byte_like = req_stone.content

    stone = pd.read_csv( BytesIO(stone_byte_like) )
    stone['mes'] = pd.to_datetime(stone['mes'])
    
    cols_num = ['indice_stone', 'indice_stone_yoy_change',
           'indice_stone_seasonally_adj',
           'indice_stone_seasonally_adj_mom_change']
    
    for col in cols_num:
        stone[col] = stone[col].str.replace(',', '.').astype(float)
    
    # ---------------------------------------------------------------------------------
    
    # Aplica filtros de data
    if start != None and type(start) == str:
        stone = stone.query(f"mes >= '{start}'")

    if end != None and type(end) == str:
        stone = stone.query(f"mes <= '{end}'")
        
    # -----------------------------------------------------------------------------------
    
    # Aplica filtro de localização
    if uf_pais == 'all':
        pass
    elif uf_pais in stone['uf / país'].unique():
        stone = stone.query(f" `uf / país` == '{uf_pais}' ")
    else:
        print(f'{uf_pais} não é um valor válido para o parâmetro ´uf_pais´.')
    
    # -------------------------------------------------------------------------------------

    # Aplica filtro de região

    if regiao == 'all':
        pass
    elif regiao in stone['região'].unique():
        stone = stone.query(f" região == '{regiao}' ")
    else:
        print(f'{regiao} não é um valor válido para o parâmetro ´regiao´.')

    # -------------------------------------------------------------------------------------

    # Aplica filtro de setor
    if setor == 'all':
        pass
    elif setor in stone['setor de atividade'].unique():
        stone = stone.query(f" `setor de atividade` == '{setor}' ")
    else:
        print(f'{setor} não é um valor válido para o parâmetro ´setor de atividade´.')

    # -------------------------------------------------------------------------------------

    # Aplica filtro de tipo
    if tipo == 'all':
        pass
    elif tipo in stone['tipo'].unique():
        stone = stone.query(f" tipo == '{tipo}' ")
    else:
        print(f"'{tipo}' não é um valor válido para o parâmetro ´tipo´.")

    # ------------------------------------------------------------------------------------
    # Determina se o index será a coluna 'mes' ou não
    if date_index == True:
        stone.set_index('mes', inplace=True)
    
    else:
        stone.reset_index(drop=True, inplace=True)
    
    return stone


# get_cielo
def get_cielo(
    indice: str = 'Mensal',  # Frequência do índice (Mensal, Trimestral, Semestral, Anual)
    local: str = 'all',      # Localidade a filtrar
    in_pct: bool = False,   # Se True, converte os valores para porcentagem
    date_index: bool = True  # Se True, a coluna 'Data' será o índice
            ):
    """
    Recupera os dados do Índice Cielo de Vendas (ICVA) e retorna um DataFrame do Pandas.

    Args:
        indice: Frequência do índice a ser recuperado.
        local: Localidade a filtrar. Use 'all' para incluir todos.
        in_pct: Se True, converte os valores para porcentagem.
        date_index: Se True, a coluna 'Data' será o índice do DataFrame.

    Returns:
        pd.DataFrame: DataFrame com os dados do ICVA, filtrados e formatados.

    Raises:
        NameError: Se o valor de `indice` for inválido.
    """
    import pandas as pd
    import datetime as dt
    import wget
    import os

    # Calcula a data do mês passado para construir a URL
    hoje = dt.date.today()  # Obtém a data de hoje
    data_mes_passado = hoje - pd.offsets.MonthBegin(2)  # Subtrai 2 meses para obter o início do mês passado
    mes_passado = data_mes_passado.month  # Extrai o número do mês
    ano_mes_passado = data_mes_passado.year  # Extrai o ano

    # Dicionário para mapear números de meses para suas abreviações
    meses = {
                1: 'Jan', 2: 'Fev', 3: 'Mar',
                4: 'Abr', 5: 'Mai', 6: 'Jun',
                7: 'Jul', 8: 'Ago', 9: 'Set',
                10: 'Out', 11: 'Nov', 12: 'Dez'
            }
    
    # Constrói a URL completa para o download do arquivo Excel
    url = f"https://www.cielo.com.br/docs/inteligencia-de-dados/{ano_mes_passado}/Historico_ICVA_{meses[mes_passado]}_{data_mes_passado.strftime('%y')}.xlsx"

    # Baixa o arquivo Excel e determina o nome do arquivo
    arq_name = wget.detect_filename(url) # Obtém o nome do arquivo automaticamente a partir da URL
    wget.download(url, arq_name) # Baixa o arquivo para o diretório atual

    indice = indice.title()

    if indice in ['Mensal', 'Trimestral', 'Semestral', 'Anual']:

        # Importação do arquivo como dataframe do pandas
        if indice == 'Mensal':
            cielo = pd.read_excel(arq_name, skiprows=6, skipfooter=1, sheet_name = 'Índice ' + indice).drop(columns='Unnamed: 0')
        else:
            cielo = pd.read_excel(arq_name, skiprows=6, sheet_name = 'Índice ' + indice).drop(columns='Unnamed: 0')

        # Formata o df para um formato mais conveniente
        cielo = (
                    cielo
                    .melt(id_vars=['Setor', 'Localidade', 'Visão'], var_name='Data', value_name='ICVA')
                    .pivot(index=['Data', 'Setor', 'Localidade'], columns='Visão', values='ICVA')
                    .reset_index()
                )
        
        # -------------------- Ajuste nos nomes das colunas ----------------------------------
        cielo.columns.name = ''
        cielo.columns = [col.strip() for col in cielo.columns] # Retira espaços extras

        # -------------------- Ajustes nas datas para cada índice -------------------------------
        
        if indice == 'Mensal':
            cielo['Data'] = pd.to_datetime(cielo['Data']) # Ajusta a coluna para tipo data
        
        elif indice == 'Trimestral':
            cielo.rename(columns={'Data':'Trimestre'}, inplace = True) # Renomeia coluna

            # Ajuste ns coluna para ser reconhecida como período
            cielo['Trimestre'] = cielo['Trimestre'].str.replace('T', 'Q') 
            cielo['Trimestre'] = pd.PeriodIndex(cielo['Trimestre'], freq='Q')

            # Criação de nova coluna do tipo data
            cielo = cielo.assign( Data = cielo['Trimestre'].map( lambda x: x.strftime("%Y-%m") + '-01' ) )    
            cielo['Data'] = pd.to_datetime(cielo['Data'])
            
            cielo['Trimestre'] = list( map( lambda x: str(x), cielo['Trimestre'] ) ) # Mantém a coluna como string
            cielo = cielo.reindex(columns=['Data', 'Trimestre', 'Setor', 'Localidade', 'Deflacionado', 'Nominal']) # Organiza as colunas
        
        elif indice == 'Semestral':
            cielo.rename(columns={'Data':'Semestre'}, inplace = True) # Renomeia a coluna

            # Cria nova coluna de data
            cielo['year'] = ('20' + cielo['Semestre'].str[-2:]).astype(int)
            cielo['mes'] = [6 if i[0] == '1' else 12 for i in cielo['Semestre']  ]
            cielo['Data'] = [dt.date(ano, mes, 1) for ano, mes in zip(cielo['year'], cielo['mes'])]
            
            cielo = cielo.reindex(columns = ['Data', 'Semestre', 'Setor', 'Localidade', 'Deflacionado', 'Nominal']) # Organiza as colunas
            
        elif indice == 'Anual':
            # Não considerei relevante fazer algum tipo de ajuste na data para o índice anual
            pass

        # ----------------------------------------------------------------------------------------------
        # Coluna 'Data' vira indice
        if date_index == True:
            cielo.set_index('Data', inplace=True)

        # Colunas numéricas como percentual
        if in_pct == True:
            if indice != 'Mensal':
                cielo[['Deflacionado', 'Nominal']] = cielo[['Deflacionado', 'Nominal']] * 100
            
            else:
                num_cols = ['Deflacionado - Com Ajuste Calendário', 'Deflacionado - Sem Ajuste Calendário',
                            'Nominal - Com Ajuste Calendário', 'Nominal - Sem Ajuste Calendário']
                
                cielo[num_cols] = cielo[num_cols] * 100
        # ---------------------------------------------------------------------------------------------
        # -------------------------------------- Filtro de localidade ---------------------------------
        
        lista_localidade = list( cielo['Localidade'].unique() ) # Cria lista de localidades válidas
        
        if local == 'all':
            pass # Não aplica nenhum filtro
            
        elif local in lista_localidade:
            cielo = cielo.query(f"Localidade == '{local}'") # Aplica filtro
            
        else:
            print(f"\n '{local}' não é um valor válido para o parâmetro ´local´. Tente um do seguintes: ")
            print(f"{['all'] + lista_localidade}")
            
        # Deleta o arquivo baixado
        os.remove(arq_name)
        
        return cielo
    
    else:
        os.remove(arq_name)
        raise NameError(f"'{indice}' não é um valor válido para o parâmetro ´indice´. Tente um dos seguintes: ['Mensal', 'Trimestral', 'Semestral', 'Anual']")
    

# get_ipea
def get_ipea(series: dict, 
             start: str | None = None, 
             end: str | None = None):
    
    import pandas as pd
    import ipeadatapy as ipea
    
    final_df = pd.DataFrame()
    
    for nome, code in series_dict.items():
        df_temp = (ipea.timeseries(series = code, )
                   .iloc[:, [0, 5] ]
                   .reset_index()
                   )
        df_temp.columns = ['Date', 'Indicador', 'Valor']
        
        df_temp['Indicador'] = nome
        
        final_df = pd.concat([final_df, df_temp], ignore_index=True)
        
    return final_df
