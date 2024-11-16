import pandas as pd
import warnings
from rapidfuzz import process, fuzz
from linguagens_e_frameworks.linguagens_e_frameworks import FRAMEWORKS_POPULARES

warnings.filterwarnings("ignore", 
                        message="Could not infer format, so each element will be parsed individually, "
                        "falling back to `dateutil`")

def formatar_telefone(numero):
    """Formata números de telefone para o formato +XXXXXXXXXXXXXXX.
    Adiciona o código do país +55 se não estiver presente e remove caracteres não numéricos, exceto "+".
    """
    numero = ''.join(c for c in numero if c.isdigit() or c == '+')
    if numero.startswith('+'):
        numero_formatado = numero
    else:
        numero_formatado = '+55' + numero
    numero_formatado = ''.join(c for c in numero_formatado if c.isdigit())
    return f"+{numero_formatado}"

def formatar_frameworks(frameworks):
    """Formata a lista de frameworks e bibliotecas.
    Converte cada item para o formato de título e remove espaços extras, mantendo a ordem original.
    Se o item começar com um ponto seguido de um espaço, o ponto é unido com a próxima letra.
    """
    if isinstance(frameworks, str) and len(frameworks) > 2:
        frameworks_list = [f.strip().title() for f in frameworks.split(',')]
        formatted_list = []
        for f in frameworks_list:
            if f.startswith('. '):
                formatted_list.append('.' + f[2:])
            else:
                formatted_list.append(f)
        return ', '.join(formatted_list)
    else:
        return ''

def corrigir_frameworks_e_linguagens(linha):
    frameworks_corrigidos = []    
    linha_lower = linha.lower()

    if '/' in linha_lower:
        partes = linha_lower.split('/')
    else:
        partes = [linha_lower]
    
    for parte in partes:
        palavras = parte.split()

        for palavra in palavras:
            resultado = process.extractOne(palavra, [f.lower() for f in FRAMEWORKS_POPULARES], scorer=fuzz.ratio)
            
            if resultado and resultado[1] >= 60:
                frameworks_corrigidos.append(FRAMEWORKS_POPULARES[[f.lower() for f in FRAMEWORKS_POPULARES].index(resultado[0])])

    frameworks_ordenados = sorted(set(frameworks_corrigidos))
    return ', '.join(frameworks_ordenados)

def transformar_dados():
    """ Retorna um dataframe com as keys: ['timestamp', 'nome_completo', 'whatsapp_numero', 'interesse_voluntario',
       'nivel_experiencia', 'areas_interesse', 'linguagens_ferramentas',
       'frameworks_bibliotecas', 'disponibilidade_horario',
       'preferencias_colaboracao', 'liderar_projetos']"""

    diretorio_local = 'Data/inscritos_hackathon.csv'
    try:
        df_planilha_hackaton = pd.read_csv(diretorio_local, sep=',')
    except FileNotFoundError:
        print(f"Erro: O arquivo {diretorio_local} não foi encontrado.")
    except pd.errors.EmptyDataError:
        print("Erro: O arquivo está vazio.")
    except pd.errors.ParserError:
        print("Erro: Ocorreu um erro ao analisar o arquivo.")
    except Exception as e:
        print(f"Erro inesperado: {e}")
    
    df_planilha_hackaton.rename(columns={
        'Timestamp': 'timestamp',
        'Nome Completo ': 'nome_completo',
        '  E-mail  ': 'email',
        'Telefone  ': 'telefone',
        'Qual é o seu papel atual na Comunidade?': 'papel_atual_comunidade',
        'Caso você esteja aguardando ser alocado em uma equipe, insira a URL da sua Trilha ': 'url_trilha_aguardando',
        'Qual área você gostaria de atuar?  ': 'area_atuacao',
        'Qual linguagem e frameworks você tem mais familiaridade? (Liste apenas termos, como [Python, Django, etc.])': 'linguagem_frameworks',
        'Disponibilidade de Horário:': 'disponibilidade_horario',
        'Você está ciente de que o hackathon exige comprometimento contínuo com sua equipe e o projeto durante as duas próximas semanas?': 'comprometimento_hackathon',
        'Você está preparado para trabalhar em equipe, colaborando efetivamente e comunicando-se de forma clara com os membros da sua equipe?': 'preparado_trabalhar_em_equipe'
    }, inplace=True)

    df_planilha_hackaton['timestamp'] = pd.to_datetime(df_planilha_hackaton['timestamp'], errors='coerce')
    df_planilha_hackaton['timestamp'] = df_planilha_hackaton['timestamp'].dt.strftime('%d/%m/%Y - %H:%M:%S')
    df_planilha_hackaton['nome_completo'] = df_planilha_hackaton['nome_completo'].str.title()
    df_planilha_hackaton['telefone'] = df_planilha_hackaton['telefone'].apply(formatar_telefone)
    df_planilha_hackaton['linguagem_frameworks'] = df_planilha_hackaton['linguagem_frameworks'].apply(formatar_frameworks)    
    df_planilha_hackaton = df_planilha_hackaton.drop_duplicates(subset='email', keep='first')
    df_planilha_hackaton = df_planilha_hackaton.drop(columns=['email', 'telefone'])
    df_planilha_hackaton['linguagem_frameworks'] = df_planilha_hackaton['linguagem_frameworks'].apply(corrigir_frameworks_e_linguagens)
    return df_planilha_hackaton
    

# if __name__ == '__main__':
#     data = transformar_dados()
#     print(data.head())
#     data.to_csv('dados_transformados.csv')