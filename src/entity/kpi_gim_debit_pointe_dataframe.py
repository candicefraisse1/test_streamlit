from enum import Enum

class KpiGimDebitPointeDataframe(Enum):
    SNOWFLAKE_TABLE_NAME = "KPI_GIM_DEBIT_POINTE"
    JOURNEE = "JOURNEE"
    SITE = "SITE"
    COMBUSTIBLE = "COMBUSTIBLE"
    UNITE = "UNITE"
    VALEUR_PREVISIONNELLE = "VALEUR_PREVISIONNELLE"
    VALEUR_REELLE = "VALEUR_REELLE"
    VALEUR_CONSOLIDEE = "VALEUR_CONSOLIDEE"

    LIST_COMBINAISON_SITE_COMBUSTIBLE_UNITE = [
            ["SAINT_OUEN_1", "GAZ", "tv"],
            ["SAINT_OUEN_2", "CHARBON_ET_BOIS", "tv"],
            ["SAINT_OUEN_3", "GAZ_AA", "tv"],
            ["SAINT_OUEN_3", "GAZ_RS","tv"],
            ["SAINT_OUEN_3", "GAZ_PC","tv"],
            ["SAINT_OUEN_3", "ELECTRICITE_EXPORTEE", "kwh"],
            ["BERCY", "GAZ", "tv"],
            ["BERCY", "BIOGAZ", "tv"],
            ["BERCY", "BIO_COMB_LIQUIDE", "tv"],
            ["GRENELLE", "GAZ", "tv"],
            ["GRENELLE", "BIOGAZ", "tv"],
            ["GRENELLE", "BIO_COMB_LIQUIDE","tv"],
            ["VAUGIGARD", "GAZ", "tv"],
            ["VAUGIGARD", "BIOGAZ","tv"],
            ["IVRY", "GAZ","tv"],
            ["IVRY", "BIOGAZ","tv"],
            ["KB", "GAZ", "tv"],
            ["VITRY", "GAZ_AA", "tv"],
            ["VITRY", "GAZ_RS", "tv"],
            ["VITRY", "GAZ_PC", "tv"],
            ["VITRY", "ELECTRICITE_EXPORTEE", "kwh"],
            ["SALPETRIERE", "GAZ","tv"],
            ["SYCTOM_IP13", "OM_IP13","tv"],
            ["SYCTOM_ISSEANE", "OM_ISSEANE","tv"],
            ["SYCTOM_ST_OUEN", "OM_ST_OUEN","tv"]
        ]
