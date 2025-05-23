import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from collections import Counter
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import tempfile
import json # Non utilisé pour charger, mais si vous avez besoin de parser JSON pour d'autres raisons, il reste
import pytz

# === CONFIGURATION ===
# Les scopes nécessaires pour gspread et Google Drive
# gspread utilise "https://spreadsheets.google.com/feeds"
# Google Drive API pour uploader utilise "https://www.googleapis.com/auth/drive"
SCOPES = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Dossier ID où tu souhaites enregistrer le fichier sur Google Drive.
# Pour une meilleure portabilité, ce FOLDER_ID pourrait aussi être dans st.secrets si il varie par environnement.
FOLDER_ID = "1euVnfDZbsquY-iMZR7_GIeyA2_4zbKsq"

# Colonnes à garder
COLONNES_UTILISEES = [
    "Classe", "Classe Groupe", "Nom", "Prénom", "Date De Naissance", "Nom / Prénom de l'élève", "Genre",
    "PersonneID", "Responsable 1 Nom", "Responsable 1 Prénom", "Responsable 1 Titre", "Responsable 1 Rue",
    "Responsable 1 Numéro", "Responsable1_BP", "Responsable 1 Localité", "Responsable 1 CP"
]

# --- Fonctions utilitaires ---

def extract_sheet_id(url):
    """Extraire l'ID du Google Sheet à partir de son URL"""
    if "/d/" in url:
        return url.split("/d/")[1].split("/")[0]
    return None

def make_headers_unique(headers):
    """Rendre les en-têtes uniques si nécessaire"""
    count = Counter()
    result = []
    for h in headers:
        h = h.strip()
        if count[h] == 0:
            result.append(h)
        else:
            result.append(f"{h}_{count[h]}")
        count[h] += 1
    return result

# --- Fonction d'authentification Google modifiée pour utiliser st.secrets ---
@st.cache_resource
def get_google_credentials():
    """
    Récupère les identifiants du compte de service Google depuis st.secrets
    et retourne un objet ServiceAccountCredentials (pour gspread) et les identifiants bruts (pour googleapiclient).
    """
    required_keys = [
        "type", "project_id", "private_key_id", "private_key",
        "client_email", "client_id", "auth_uri", "token_uri",
        "auth_provider_x509_cert_url", "client_x509_cert_url",
        "universe_domain"
    ]

    creds_info = {}
    
    # Vérification de l'existence de la section des secrets Google
    if "google_service_account" not in st.secrets:
        st.error("Erreur de configuration : La section '[google_service_account]' est manquante dans vos secrets Streamlit. "
                 "Veuillez vous assurer que votre fichier secrets.toml sur Streamlit Cloud est correctement configuré.")
        st.stop() # Arrête l'application si les secrets ne sont pas trouvés

    # Récupération de chaque clé sous la section 'google_service_account'
    for key in required_keys:
        if key not in st.secrets["google_service_account"]:
            st.error(f"Erreur de configuration : La clé '{key}' est manquante "
                     f"dans la section '[google_service_account]' de vos secrets Streamlit.")
            st.stop()
        creds_info[key] = st.secrets["google_service_account"][key]

    try:
        # gspread utilise ServiceAccountCredentials
        gspread_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, SCOPES)
        
        # googleapiclient.discovery.build utilise un objet Credentials directement
        # Le format de creds_info est déjà ce dont il a besoin, mais on va le convertir
        # en un objet google.oauth2.service_account.Credentials pour plus de clarté
        google_api_creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, SCOPES)

        return gspread_creds, google_api_creds
    except Exception as e:
        st.error(f"Erreur lors de la création des identifiants Google. "
                 f"Vérifiez le format de votre clé privée et l'ensemble des informations de votre compte de service. Détail : {e}")
        st.stop() # Arrête l'application en cas d'échec d'authentification

# --- Fonctions de chargement et création des feuilles de calcul ---

def charger_dataframe_depuis_google_sheet(url, gspread_client):
    """Charger les données depuis un Google Sheet"""
    sheet_id = extract_sheet_id(url)
    if not sheet_id:
        st.error("URL de Google Sheet invalide. Assurez-vous qu'elle contient '/d/'.")
        return None
    try:
        sheet = gspread_client.open_by_key(sheet_id).sheet1
        all_values = sheet.get_all_values()
        if not all_values:
            st.warning("La feuille est vide.")
            return pd.DataFrame()
        headers = make_headers_unique(all_values[0])
        data = all_values[1:]
        df = pd.DataFrame(data, columns=headers)
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement de la feuille : {e}. "
                 "Vérifiez que le compte de service a les permissions d'accès au Google Sheet.")
        return None

def get_drive_service(google_api_creds):
    """Retourne un service Google Drive authentifié"""
    return build('drive', 'v3', credentials=google_api_creds)

def create_spreadsheet_with_data(title, df_filtered, google_api_creds, folder_id=FOLDER_ID):
    """Créer une feuille de calcul Google Sheets avec les données filtrées"""
    try:
        # Créer un fichier CSV temporaire
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv", mode='w', encoding='utf-8') as temp_file:
            df_filtered.to_csv(temp_file.name, index=False)
            temp_file_path = temp_file.name # Garder le chemin pour MediaFileUpload

        metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.spreadsheet', # Type MIME pour Google Sheets
            'parents': [folder_id]
        }

        media = MediaFileUpload(temp_file_path, mimetype='text/csv', resumable=True)
        drive_service = get_drive_service(google_api_creds)
        
        file = drive_service.files().create(body=metadata, media_body=media, fields='id').execute()
        
        # Supprimer le fichier temporaire après l'upload
        os.remove(temp_file_path)

        return file.get("id")

    except Exception as e:
        st.error(f"Erreur lors de la création de la feuille Google Sheets : {e}. "
                 "Vérifiez que le compte de service a les permissions d'écriture dans le dossier Drive spécifié.")
        return None

# === Interface Streamlit ===
st.set_page_config(
    page_title="ENVOL : Gestion de données Google Sheets",
    page_icon="📊",
    layout="centered",
    initial_sidebar_state="auto"
)

st.title("ENVOL : toute l'école avec mes items de prédilection")
st.markdown("---")

st.info("Cette application charge les données d'un Google Sheet, les filtre, puis les exporte vers un nouveau Google Sheet dans un dossier Drive spécifié. "
        "Les identifiants Google sont gérés de manière sécurisée via les secrets Streamlit.")

# Récupération des identifiants au démarrage de l'application
gspread_creds, google_api_creds = get_google_credentials()
client = gspread.authorize(gspread_creds) # Initialisation du client gspread une seule fois

url_sheet = st.text_input("🔗 Veuillez coller l'URL du fichier Google Sheet à traiter : ")

if url_sheet:
    st.info("📥 Chargement du fichier Google Sheet...")
    df = charger_dataframe_depuis_google_sheet(url_sheet, client)

    if df is not None:
        colonnes_disponibles = [col for col in COLONNES_UTILISEES if col in df.columns]
        
        if not colonnes_disponibles:
            st.warning("Aucune des colonnes requises n'a été trouvée dans votre Google Sheet. "
                       "Veuillez vérifier les noms des colonnes dans le Google Sheet source.")
            st.dataframe(df.head()) # Afficher les colonnes brutes pour aider au débogage
        else:
            df_filtré = df[colonnes_disponibles]

            st.success(f"🔍 Colonnes retenues : {', '.join(colonnes_disponibles)}")
            st.dataframe(df_filtré.head())  # Afficher un aperçu des données filtrées

            nom_utilisateur = st.text_input("📝 Entrez un nom pour le fichier Google Sheet généré : ")

            if nom_utilisateur:
                if st.button("Créer le Google Sheet avec les données filtrées"):
                    with st.spinner("Création du Google Sheet en cours..."):
                        # Utiliser pytz pour ajuster le fuseau horaire
                        fuseau_horaire_local = pytz.timezone('Europe/Brussels') # Fuseau horaire pour Mons, Belgique
                        timestamp = pd.to_datetime("now", utc=True).tz_convert(fuseau_horaire_local).strftime("%Y-%m-%d_%Hh%M")

                        # Générer le nom du fichier avec la date et l'heure locale
                        nouveau_nom = f"{nom_utilisateur} - {timestamp}"
                        st.info(f"📝 Nom du fichier final : {nouveau_nom}")

                        file_id = create_spreadsheet_with_data(nouveau_nom, df_filtré, google_api_creds)

                        if file_id:
                            st.success(f"✅ Nouveau fichier créé : [Ouvrir le fichier]({f'https://docs.google.com/spreadsheets/d/{file_id}'})")
                            st.info(f"📁 Fichier enregistré dans le dossier Google Drive ID : {FOLDER_ID}")
                        else:
                            st.error("❌ La création du fichier Google Sheet a échoué.")
            else:
                st.warning("Veuillez entrer un nom pour le fichier généré.")
    else:
        st.warning("Impossible de charger les données du Google Sheet. Veuillez vérifier l'URL et les permissions.")
else:
    st.info("Veuillez coller l'URL de votre Google Sheet ci-dessus pour commencer.")

st.markdown("---")
st.markdown("Développé avec ❤️ pour ENVOL via Streamlit et Google APIs")
