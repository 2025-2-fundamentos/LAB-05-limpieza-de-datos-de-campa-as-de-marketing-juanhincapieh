"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    import pandas as pd
    from pathlib import Path

    inputDir = Path("files/input")
    outputDir = Path("files/output")
    outputDir.mkdir(parents=True, exist_ok=True)

    dfList = []
    for filePath in inputDir.glob("*.csv.zip"):
        df = pd.read_csv(filePath, compression="zip", sep=",")
        dfList.append(df)

    if not dfList:
        return

    fullDf = pd.concat(dfList, ignore_index=True)

    clientDf = fullDf[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ].copy()

    clientDf["job"] = (
        clientDf["job"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    clientDf["education"] = (
        clientDf["education"]
        .astype(str)
        .str.replace(".", "_", regex=False)
    )
    clientDf["education"] = clientDf["education"].replace("unknown", pd.NA)

    clientDf["credit_default"] = (
        clientDf["credit_default"].astype(str).str.strip().str.lower() == "yes"
    ).astype(int)

    clientDf["mortgage"] = (
        clientDf["mortgage"].astype(str).str.strip().str.lower() == "yes"
    ).astype(int)

    clientDf = clientDf[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ]

    clientDf.to_csv(outputDir / "client.csv", index=False)

    campaignDf = fullDf[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()

    prevSeries = campaignDf["previous_outcome"].astype(str).str.strip().str.lower()
    campaignDf["previous_outcome"] = (prevSeries == "success").astype(int)

    campSeries = campaignDf["campaign_outcome"].astype(str).str.strip().str.lower()
    campaignDf["campaign_outcome"] = (campSeries == "yes").astype(int)

    dateSeries = (
        campaignDf["day"].astype(str)
        + "-"
        + campaignDf["month"].astype(str)
        + "-2022"
    )
    lastContactDate = pd.to_datetime(dateSeries, format="%d-%b-%Y", errors="coerce")
    campaignDf["last_contact_date"] = lastContactDate.dt.strftime("%Y-%m-%d")

    campaignDf = campaignDf[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
    ]

    campaignDf.to_csv(outputDir / "campaign.csv", index=False)

    economicsDf = fullDf[
        [
            "client_id",
            "cons_price_idx",
            "euribor_three_months",
        ]
    ].copy()

    economicsDf = economicsDf[
        [
            "client_id",
            "cons_price_idx",
            "euribor_three_months",
        ]
    ]

    economicsDf.to_csv(outputDir / "economics.csv", index=False)


if __name__ == "__main__":
    clean_campaign_data()
