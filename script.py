import streamlit as st
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from collections import Counter
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import tempfile
import json  # Pour charger le JSON depuis le fichier téléchargé
import pytz

# === CONFIGURATION ===
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Dossier ID où tu souhaites enregistrer le fichier sur Google Drive
FOLDER_ID = "1euVnfDZbsquY-iMZR7_GIeyA2_4zbKsq"

# Colonnes à garder
COLONNES_UTILISEES = [
    "Classe", "Classe Groupe", "Nom", "Prénom", "Date De Naissance", "Nom / Prénom de l'élève", "Genre",
    "PersonneID", "Responsable 1 Nom", "Responsable 1 Prénom", "Responsable 1 Titre", "Responsable 1 Rue",
    "Responsable 1 Numéro", "Responsable1_BP", "Responsable 1 Localité", "Responsable 1 CP"
]


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


def charger_dataframe_depuis_google_sheet(url, client):
    """Charger les données depuis un Google Sheet"""
    sheet_id = extract_sheet_id(url)
    try:
        sheet = client.open_by_key(sheet_id).sheet1
        all_values = sheet.get_all_values()
        headers = make_headers_unique(all_values[0])
        data = all_values[1:]
        df = pd.DataFrame(data, columns=headers)
        return df
    except Exception as e:
        st.error(f"Erreur lors du chargement de la feuille : {e}")
        return None


def get_drive_service(creds):
    """Retourne un service Google Drive authentifié"""
    return build('drive', 'v3', credentials=creds)


def create_spreadsheet_with_data(title, df_filtered, creds, folder_id=FOLDER_ID):
    """Créer une feuille de calcul Google Sheets avec les données filtrées"""

    try:
        temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        df_filtered.to_csv(temp_file.name, index=False)

        metadata = {
            'name': title,
            'mimeType': 'application/vnd.google-apps.spreadsheet',
            'parents': [folder_id]
        }

        media = MediaFileUpload(temp_file.name, mimetype='text/csv', resumable=True)
        drive_service = get_drive_service(creds)
        file = drive_service.files().create(body=metadata, media_body=media, fields='id').execute()
        return file.get("id")

    except Exception as e:
        st.error(f"Erreur lors de la création de la feuille : {e}")
        return None


# === Interface Streamlit ===
st.title("ENVOL : toute l'école avec mes items de prédilection")

uploaded_file = st.file_uploader("Téléchargez votre fichier JSON de clé privée", type="json")

if uploaded_file is not None:
  try:
    # Charger le contenu du fichier JSON
    creds_info = json.load(uploaded_file)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_info, scope)
    client = gspread.authorize(creds)

    url_sheet = st.text_input("🔗 Veuillez coller l'URL du fichier Google Sheet à traiter : ")

    if url_sheet:
        st.info("📥 Chargement du fichier Google Sheet...")
        df = charger_dataframe_depuis_google_sheet(url_sheet, client)

        if df is not None:
            colonnes_disponibles = [col for col in COLONNES_UTILISEES if col in df.columns]
            df_filtré = df[colonnes_disponibles]

            st.success(f"🔍 Colonnes retenues : {colonnes_disponibles}")
            st.dataframe(df_filtré.head())  # Afficher un aperçu

            nom_utilisateur = st.text_input("📝 Entrez un nom pour le fichier généré : ")

            if nom_utilisateur:
                # Utiliser pytz pour ajuster le fuseau horaire
                fuseau_horaire_local = pytz.timezone('Europe/Paris')  # À adapter à votre fuseau horaire local
                timestamp = pd.to_datetime("now", utc=True).tz_convert(fuseau_horaire_local).strftime("%Y-%m-%d_%Hh%M")

                # Générer le nom du fichier avec la date et l'heure locale
                nouveau_nom = f"{nom_utilisateur} - {timestamp}"
                st.info(f"📝 Nom du fichier final : {nouveau_nom}")

                file_id = create_spreadsheet_with_data(nouveau_nom, df_filtré, creds)

                if file_id:
                    st.success(f"✅ Nouveau fichier créé : https://docs.google.com/spreadsheets/d/{file_id}")
                    st.info(f"📁 Fichier enregistré dans le dossier Google Drive ID : {FOLDER_ID}")

except Exception as e:
    st.error(f"Une erreur s'est produite lors du traitement : {e}")

else:
    st.warning("Veuillez télécharger votre fichier JSON de clé privée pour continuer.")
