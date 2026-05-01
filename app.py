from flask import Flask, jsonify
import requests
import pandas as pd
import re
import numpy as np
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time as tm
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
app = Flask(__name__)


def buscar_dados():
    link = "8101188074:AAHkS6A28wGm7JAkXuUBOlogi5k7x3w5fno"

    html = requests.get(link, timeout=20).text
    df = pd.read_html(html)[0]

    # Exemplo: transforma o DataFrame em lista de dicionários
    resultado = df.fillna("").to_dict(orient="records")

    return resultado


@app.route("/")
def home():
    return "Aplicação rodando no Render!"


@app.route("/dados")
def dados():
    try:
        resultado = buscar_dados()
        return jsonify(resultado)
    except Exception as erro:
        return jsonify({"erro": str(erro)}), 500
if __name__ == "__main__":
    app.run(debug=True)
ligas = [
    "England Premier League",
    "England Championship",
    "Italy Serie A",
    "Italy Serie B",
    "Spain La Liga",
    "Spain Segunda",
    "Germany Bundesliga I",
    "Germany Bundesliga II",
    "France Ligue 1",
    "France Ligue 2",
    "Austria Bundesliga",
    "Belgium First Division A",
    "Croatia 1.HNL",
    "Czech First League",
    "Holland Eredivisie",
    "Hungary NB I",
    "Poland Ekstraklasa",
    "Portugal Primeira Liga",
    "Romania Liga I",
    "Russia Premier League",
    "Slovenia Prva Liga",
    "Switzerland Super League",
    "Turkey Super Lig",
    "Ukraine Persha Liga",
    "Sweden Allsvenskan",
    "League of Ireland Premier Division",
    "Denmark Superligaen",
    "Japan J-League",
    "South Korea K-League 1",
    "South Africa Premier",
    "Qatar Stars League",
    "Argentina Primera Division",
    "Chile Apertura",
    "USA MLS",
    "Norway Tippeligaen",
    "Mexico Apertura",
    "China Super League",
    "Brazil Serie A",
    "Brazil Serie B",
    "UEFA Champions League",
    "UEFA Europa League",
    "Wales Premier League",
    "Scotland Premiership",
    "AFC Champions League",
    "Australia A-League",
    "Asia - World Cup Qualifying",
    "South America - World Cup Qualifying",
    "Euro 2024 Qualifying"
    "Copa Libertadores",
    "Copa Sudamericana"
    ]

def normalizar(txt):
    txt = str(txt).strip().lower()
    txt = re.sub(r'\s+', ' ', txt)
    txt = re.sub(r'^(?:\d+\s+)+', '', txt)      # remove números do começo
    txt = re.sub(r'(?:\s+\d+)+$', '', txt)      # remove números do fim
    return txt.strip()

def pegar_links_times(df):
    url = 'https://www.totalcorner.com/match/today'
    headers = {"User-Agent": "Mozilla/5.0"}

    html = requests.get(url, headers=headers, timeout=20).text
    soup = BeautifulSoup(html, 'html.parser')

    base = 'https://www.totalcorner.com'
    links_times = {}

    for a in soup.find_all('a', href=True):
        href = a['href']
        nome = normalizar(a.get_text())

        if '/team/view/' in href and nome:
            links_times[nome] = urljoin(base, href)

    # pega todos os times do dataframe
    times_unicos = pd.unique(
        df[['Home', 'Away']].astype(str).values.ravel()
    )

    resultado = {}
    for time in times_unicos:
        if str(time).lower() == 'nan':
            continue
        time_norm = normalizar(time)
        resultado[time] = links_times.get(time_norm)

    return resultado

def dfs_por_times(links_times):
    dfs_por_time = {}

    for time_nome, link in links_times.items():
        try:
            data_dos_links = requests.get(link)
            tabelas=pd.read_html(data_dos_links.text)[0]
            tabelas.drop(columns=['Time','Handicap','Corner','Corner O/U','Total Goals','Goals O/U','Tips','Dangerous Attack','Live Events','Analysis'], inplace=True)
            tabelas.dropna(axis=0, how='any', inplace=True)
            tabelas.rename(columns={'Unnamed: 2':'cronometro'}, inplace=True)
            tabelas = tabelas[tabelas['cronometro'] == 'Full']
            tabelas['Score']=tabelas['Score'].apply(lambda a: re.findall(r'\d{1,} - \d{1,}',a))
            tabelas['Score']=tabelas['Score'].apply(lambda a: list(map(int, a[0].split(' - '))))
            tabelas['Home'] = tabelas['Home'].apply(lambda a: re.sub(r'^(?:\d+\s+)+', '', str(a)).strip())
            tabelas['Away'] = tabelas['Away'].apply(lambda a: re.sub(r'(?:\s+\d+)+$', '', str(a)).strip())
            tabelas['Time_Buscado'] = time_nome
            dfs_por_time[time_nome] = tabelas
        except Exception as e:
            print(f"Erro em {time_nome}: {e}")

    return dfs_por_time

def limpar_time(txt):
    txt = str(txt).replace('\xa0', ' ').strip()

    # remove blocos no começo: 1 , 2 , [9] , [12] ...
    txt = re.sub(r'^(?:\s*(?:\[\d+\]|\d+)\s*)+', '', txt)

    # remove blocos no fim: 1 , 2 , [7] , [16] ...
    txt = re.sub(r'(?:\s*\[\d+\]\s*|\s+\d+\s*)+$', '', txt)

    return txt.strip()

buscar_dados()
CHAT_ID = "5095408254"

sinais_enviados = set()

while True:
    try:
        data = requests.get('https://www.totalcorner.com/match/today')
        df = pd.read_html(data.text)[0]
        df.dropna(axis=1, how='all', inplace=True)
        df.drop(columns=['Handicap','Corner','Total Goals', 'Dangerous Attack', 'Analysis',], inplace=True)
        df.rename(columns={'Unnamed: 3': 'cronometro'}, inplace=True)
        df = df[df['cronometro'].isna()].copy()
        df = df.iloc[2:].reset_index(drop=True)
        df['Home'] = df['Home'].apply(limpar_time)
        df['Away'] = df['Away'].apply(limpar_time)
        time_home = list(zip(df['League'], df['Home']))
        time_home = list(zip(df['League'], df['Home']))

        df=df[df['League'].isin(ligas)]
        links_times = pegar_links_times(df)
        dfs_times = dfs_por_times(links_times)
        df_final = pd.concat(dfs_times.values(), ignore_index=True)
        # =========================
        # CRIA O df_sem_gol
        # =========================
        resultado = []

        for time_buscado in df_final['Time_Buscado'].dropna().unique():
            jogos_time = df_final[df_final['Time_Buscado'] == time_buscado].copy()

            # pega os 2 jogos mais recentes
            ultimos_2 = jogos_time.head(2)

            if len(ultimos_2) < 2:
                continue

            gols_feitos = []
            gols_sofridos = []

            for _, row in ultimos_2.iterrows():
                score = row['Score']

                # Score precisa estar no formato [gols_home, gols_away]
                if not isinstance(score, list) or len(score) < 2:
                    gols_feitos.append(None)
                    gols_sofridos.append(None)
                    continue

                gols_home, gols_away = score[0], score[1]

                if row['Home'] == time_buscado:
                    gols_feitos.append(gols_home)
                    gols_sofridos.append(gols_away)

                elif row['Away'] == time_buscado:
                    gols_feitos.append(gols_away)
                    gols_sofridos.append(gols_home)

                else:
                    gols_feitos.append(None)
                    gols_sofridos.append(None)

            # condição 1: não fez gols nos últimos 2 jogos
            nao_fez_gol = gols_feitos == [0, 0]

            # condição 2: não tomou gols nos últimos 2 jogos
            nao_tomou_gol = gols_sofridos == [0, 0]

            if nao_fez_gol or nao_tomou_gol:
                ultimos_2 = ultimos_2.copy()
                ultimos_2['Time_Buscado'] = time_buscado

                if nao_fez_gol and nao_tomou_gol:
                    ultimos_2['Tipo_Sinal'] = 'Não fez gols e não tomou gols nos últimos 2 jogos'
                elif nao_fez_gol:
                    ultimos_2['Tipo_Sinal'] = 'Não fez gols nos últimos 2 jogos'
                else:
                    ultimos_2['Tipo_Sinal'] = 'Não tomou gols nos últimos 2 jogos'

                resultado.append(ultimos_2)

        if resultado:
            df_sinais = pd.concat(resultado, ignore_index=True)
        else:
            df_sinais = pd.DataFrame()
            print("Nenhum time encontrado nos critérios dos últimos 2 jogos.")


        # =========================
        # ENVIA PRO TELEGRAM
        # =========================
        if not df_sinais.empty:
            for time_buscado in df_sinais['Time_Buscado'].unique():

                jogos_time = df_sinais[
                    df_sinais['Time_Buscado'] == time_buscado
                ].reset_index(drop=True)

                if len(jogos_time) < 2:
                    continue

                tipo_sinal = jogos_time.iloc[0]['Tipo_Sinal']

                # =========================
                # ÚLTIMOS 2 JOGOS
                # =========================
                score1 = jogos_time.iloc[0]['Score']
                score2 = jogos_time.iloc[1]['Score']

                jogo1 = f"{jogos_time.iloc[0]['Home']} [{score1[0]} - {score1[1]}] {jogos_time.iloc[0]['Away']}"
                jogo2 = f"{jogos_time.iloc[1]['Home']} [{score2[0]} - {score2[1]}] {jogos_time.iloc[1]['Away']}"

                # =========================
                # JOGO ATUAL NO df
                # =========================
                jogo_atual_df = df[
                    (df['Home'] == time_buscado) | (df['Away'] == time_buscado)
                ].reset_index(drop=True)

                if not jogo_atual_df.empty:
                    row_atual = jogo_atual_df.iloc[0]

                    home_atual = row_atual['Home']
                    away_atual = row_atual['Away']
                    liga_atual = row_atual['League'] if 'League' in row_atual.index else ''
                    cronometro_atual = row_atual['cronometro'] if 'cronometro' in row_atual.index else ''

                    score_atual = row_atual['Score']

                    if isinstance(score_atual, list) and len(score_atual) >= 2:
                        placar_atual = f"[{score_atual[0]} - {score_atual[1]}]"
                    else:
                        placar_atual = str(score_atual)

                    jogo_atual = f"{home_atual} {placar_atual} {away_atual}"
                else:
                    liga_atual = ''
                    cronometro_atual = ''
                    jogo_atual = 'Jogo atual não encontrado'

                # =========================
                # EVITA REPETIR O MESMO SINAL
                # =========================
                chave_sinal = (time_buscado, tipo_sinal, jogo1, jogo2)

                if chave_sinal in sinais_enviados:
                    print(f"Sinal já enviado para {time_buscado}")
                    continue

                mensagem = f"""🚨 POSSÍVEL SINAL TELEGRAM 🚨

Time: {time_buscado}

Jogo atual:
• {jogo_atual}

Liga: {liga_atual}
Minuto: {cronometro_atual}

Últimos 2 jogos:
• {jogo1}
• {jogo2}

Critério encontrado:
{tipo_sinal}

Monitorar oportunidade no mercado de gols.
"""

                url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
                payload = {
                    "chat_id": CHAT_ID,
                    "text": mensagem
                }

                res = requests.post(url, data=payload)
                resposta = res.json()
                print(resposta)

                if resposta.get("ok"):
                    sinais_enviados.add(chave_sinal)

        print("Aguardando 5 minutos para nova checagem...")
        tm.sleep(300)

    except Exception as e:
        print("Erro no loop")
        tm.sleep(300)