from collections.abc import Iterable, Callable
import pandas as pd

CHUNK_SIZE = 100000

state_csv=pd.read_csv('state.csv',dtype=str).fillna('')
country_csv=pd.read_csv('country.csv',dtype=str).fillna('')

state_name=set(state_csv['Name']) - {''}
country_name=set(country_csv['Name']) - {''}
state_code=(set(state_csv['ANSI']) | set(state_csv['USPS']) | set(state_csv['USCG'])) - {''}
country_code=set(country_csv['Code']) - {''}

def standardize_company_names(df,
                              name_cols: str | list,
                              cleaned_cols: str | list) -> pd.DataFrame:
    if isinstance(name_cols, str):
        pass
    elif isinstance(name_cols, Iterable):
        if not isinstance(cleaned_cols, Iterable) or len(name_cols) != len(cleaned_cols):
            print("Variable Error: cleaned_cols")
            return df
        for i in range(len(name_cols)):
            df=standardize_company_names(df,name_cols[i],cleaned_cols[i])
        return df
    else:
        print("Variable Error: name_cols")
        return df

    # Step 1: Clean redundant spaces
    df[cleaned_cols] = df[name_cols].astype(str).str.upper()
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"[\u0009\u000A\u000B\u000C\u000D\u0020\u00A0\u1680\u2000-\u200A\u2028\u2029\u202F\u205F\u3000]", ' ', regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.strip()  # trim
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\s+', ' ', regex=True)  # itrim
    df[cleaned_cols] = ' ' + df[cleaned_cols] + ' '

    # Step 2: Omit punctuation
    df[cleaned_cols] = df[cleaned_cols].str.replace("AMP;", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace(".COM ", " COM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" . ", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(".", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(",", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(";", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("/", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("\\", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("'", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("`", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AND ", " & ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("&", " & ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("+", " & ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("\"", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(":", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("?", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("*", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("^", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("%", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("!", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("@", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("=", " ")

    df[cleaned_cols] = df[cleaned_cols].str.strip()  # trim
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\s+', ' ', regex=True)  # itrim
    df[cleaned_cols] = ' ' + df[cleaned_cols] + ' '

    # Step 3: Clean Compustat-related indicators:
    df[cleaned_cols] = df[cleaned_cols].str.replace("-CL A", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-DEL", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-PRO FORMA", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" -CL B", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-ADS", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-ADR", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-SVTG", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-REDH", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-NEW", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NEW $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CP $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace("-OLD", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" OLD $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CONSOLIDATED $", " ", regex=True)

    df[cleaned_cols] = df[cleaned_cols].str.strip()  # trim
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\s+', ' ', regex=True)  # itrim
    df[cleaned_cols] = ' ' + df[cleaned_cols] + ' '

    df[cleaned_cols] = df[cleaned_cols].str.replace(" AND ", " & ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("&", " & ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("+", " & ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("'", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("`", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("-", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(",", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(".COM ", " COM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" US $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" U S $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" USA $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace("\"", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" . ", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(".", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace(":", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(";", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("'", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("?", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("*", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("^", "")
    df[cleaned_cols] = df[cleaned_cols].str.replace("/", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("\\", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("%", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("!", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("@", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace("=", " ")

    # Step 4: Drop text in brackets from the name
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"\((.*)\)", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"\((.*)\)", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"\((.*)\)", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"\((.*) $", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"\[(.*)\]", " ", regex=True)

    df[cleaned_cols] = df[cleaned_cols].str.strip()  # trim
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\s+', ' ', regex=True)  # itrim
    df[cleaned_cols] = ' ' + df[cleaned_cols] + ' '

    # Step 5: Standardize the legal entity
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORPORATION ", " CORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORPORATED ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORP ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORPORATION ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PUB LTD CO ", " PLC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BANCORPORATION ", " BANCORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COS ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPANIES ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPANY ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPN ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" KONINKLIJKE ", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AKTIENGESELLSCHAFT ", " AG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AKTIENGESELL SCHAFT ", " AG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LIMITED ", " LTD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNLIMITED ", " UNLTD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CP $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" N V $", " NV ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" B V $", " BV ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" A S $", " AS ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" A G $", " AG ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" K G $", " KG ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" I N C $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" U N L T D $", " UNLTD ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L T D $", " LTD ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L L L P $", " LLLP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L L P $", " LLP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L P $", " LLP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" P L L C $", "  PLLC", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L L C $", " LLC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L P $", " LP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" G M B H $", " GMBH ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" S A R L $", " SARL ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" S P R L $", " SPRL ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" B V B A $", " BVBA ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" L D A $", " LDA ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" Z O O $", " ZOO ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" Z OO $", " ZOO ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" P T Y $", " PTY ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" S R L $", " SRL ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" P L C $", " PLC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" S A $", " SA ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" C V $", " CV ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" S DE RL $", " SDERL ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SA DE CV $", " SADECV ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" KORLATOLT FELELOSSEGU TARSASAG ", " KFT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GESELLSCHAFT MIT BESCHRAENKTER HAFTUNG ", " GMBH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GESELLSCHAFT MBH ", " GMBH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GESELLSCHAFT M B H ", " GMBH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" & COKG $", " & CO KG ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC CA $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC PA $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC WA $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC WIS $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC UTAH $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC NE $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC OHIO $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC NY $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC TENN $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC ORE $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC MASS $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC COLO $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC NJ $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC TX $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC VA $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC DEL $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INC DE $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace("INC DELAWARE $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP CA $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP PA $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP WA $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP WIS $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP UTAH $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP NE $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP OHIO $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP NY $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP TENN $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP ORE $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP MASS $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP COLO $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP NJ $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP TX $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP VA $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP DEL $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORP DE $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace("CORP DELAWARE $", " CORP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNITED STATES ", " US ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNITED KINGDOM ", " UK ")

    # Step 6: Omit "the"
    df[cleaned_cols] = df[cleaned_cols].str.replace("^ THE ", " ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" THE $", " ", regex=True)

    df[cleaned_cols] = df[cleaned_cols].str.strip()  # trim
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\s+', ' ', regex=True)  # itrim
    df[cleaned_cols] = ' ' + df[cleaned_cols] + ' '

    # Step 7: Standardize common words
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GP $", " GRP ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GP INC $", " GRP INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" IN $", " INC ", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ADVANCE ", " ADV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ADVANCED ", " ADV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AEROSPACE ", " AEROSP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AGRICULTURE ", " AGR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AGRICULTURES ", " AGR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AKTIENGESELL SCHAFT ", " AG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AKTIENGESELLSCHAFT ", " AG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AMERICA ", " AMER ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AMERICAN ", " AMER ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AMERICAS ", " AMER ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ANALYSIS ", " ANAL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ANALYTIC ", " ANALYT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ANALYTICAL ", " ANALYT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ANALYTICS ", " ANALYT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ANIMAL ", " ANIM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" APPLICATION ", " APPLICAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" APPLICATIONS ", " APPLICAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" APPLIED ", " APPL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" APPS ", " APPLICAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ASSC ", " ASSOC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ASSOCIATE ", " ASSOC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ASSOCIATES ", " ASSOC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ASSOCIATION ", " ASSOC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ASSOCIATIONS ", " ASSOC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ASSOCS ", " ASSOC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMATED ", " AUTOMAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMATIC ", " AUTOMAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMATION ", " AUTOMAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMATIVE ", " AUTOMAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMATN ", " AUTOMAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMOTIV ", " AUTOMOT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" AUTOMOTIVE ", " AUTOMOT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BANCORPORATION ", " BANCORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOLOGICAL ", " BIOL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOLOGICS ", " BIOL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOLOGY ", " BIOL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOMEDICAL ", " BIOMED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOPHARMA ", " BIOPHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOPHARMACEUT ", " BIOPHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOPHARMACEUTICAL ", " BIOPHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOPHARMACEUTICALS ", " BIOPHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOSCIENCE ", " BIOSCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOSCIENCES ", " BIOSCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOSURGICAL ", " BIOSURG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOSYST ", " BIOSYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOSYSTEM ", " BIOSYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOSYSTEMS ", " BIOSYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIO-TECH ", " BIOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTECHNOL ", " BIOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTECHNOLOGIES ", " BIOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTECHNOLOGY ", " BIOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTHERAPEUTC ", " BIOTHERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTHERAPEUTCS ", " BIOTHERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTHERAPEUTIC ", " BIOTHERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BIOTHERAPEUTICS ", " BIOTHERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CALIFORNIA ", " CALIF ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CENTER ", " CTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CENTERS ", " CTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CHEMICAL ", " CHEM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CHEMICALS ", " CHEM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CHEMS ", " CHEM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CLINICAL ", " CLIN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMM ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMS ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMUNICAT ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMUNICATI ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMUNICATION ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMUNICATIONS ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMUNICATN ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMMUNICATNS ", " COMMUN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPANIES ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPN ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPUTER ", " COMP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPUTERS ", " COMP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPUTING ", " COMP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORPORATION ", " CORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COS ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DEVELOPMENT ", " DEV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DEVELOPMENTS ", " DEV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DIAGNOSTIC ", " DIAGNOST ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DIAGNOSTICS ", " DIAGNOST ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DISPLAY ", " DISP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DISPLAYS ", " DISP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DIVISION ", " DIV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DIVISIONS ", " DIV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DVLPMNT ", " DEV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DYNAMIC ", " DYNAM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" DYNAMICS ", " DYNAM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" EDU ", " EDUC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" EDUCATION ", " EDUC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" EDUCATIONS ", " EDUC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELEC ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELECT ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELECTRIC ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELECTRICS ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELECTRONIC ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELECTRONICS ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ELECTRS ", " ELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGIN ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGINE ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGINEER ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGINEERING ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGINEERINGS ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGINEERS ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGNS ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENGR ", " ENGN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENTERPRS ", " ENTERPRISE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENVIRONMENT ", " ENVIRONM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENVIRONMENTAL ", " ENVIRONM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENVIRONMENTS ", " ENVIRONM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ENVIRONMTL ", " ENVIRONM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" EQUIPMENT ", " EQUIP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" EQUIPMENTS ", " EQUIP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" FLAVORS ", " FAVORS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GENERAL ", " GEN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GENERALS ", " GEN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GENETIC ", " GENET ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GENETICAL ", " GENET ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GENETICS ", " GENET ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GRAPHIC ", " GRAPH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GRAPHICS ", " GRAPH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GROUP ", " GRP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GROUPS ", " GRP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" GRPS ", " GRP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HEALTH ", " HLTH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HEALTH CARE ", " HLTHCR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HEALTHCARE ", " HLTHCR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HEALTH-CARE ", " HLTHCR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HLDGS ", " HLDG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HLDNGS ", " HLDG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HLDS ", " HLDG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HOLDING ", " HLDG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HOLDINGS ", " HLDG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" HOSPITAL ", " HOSP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORP ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORPORATED ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORPORATION ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INDS ", " IND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INDUS ", " IND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INDUSTRIAL ", " IND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INDUSTRIALS ", " IND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INDUSTRIES ", " IND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INDUSTRY ", " IND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INFORMAT ", " INFO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INFORMATION ", " INFO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INNOV ", " INNOVAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INNOVATION ", " INNOVAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INNOVATIONS ", " INNOVAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INNOVATIVE ", " INNOVAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INSTITUTE ", " INST ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INSTITUTES ", " INST ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INSTRS ", " INSTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INSTRUMENT ", " INSTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INSTRUMENTS ", " INSTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INT ", " INTL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTELLECTUAL PROPERTIES ", " IP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTELLECTUAL PROPERTY ", " IP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTERACTIVE ", " INTERACT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTERNATIONAL ", " INTL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTERNATIONALS ", " INTL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTERNATNAL ", " INTL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INVESTMENT ", " INVEST ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INVESTMENTS ", " INVEST ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" KONINKLIJKE ", " ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABO ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATARI ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATARIA ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATARIO ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORIE ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORIES ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORIET ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORIO ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORIOS ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORIUM ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORY ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABORATORYS ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LABS ", " LAB ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LIMITED ", " LTD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MACHINE ", " MACH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MACHINERY ", " MACH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MACHINES ", " MACH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANAGEMENT ", " MGMT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANAGEMENTS ", " MGMT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURE ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURER ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURERS ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURES ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURING ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURINGS ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MATERIAL ", " MAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MATERIALS ", " MAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MATL ", " MAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MATLS ", " MAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MECHANICAL ", " MECHAN ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MEDICAL ", " MED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MEDICALS ", " MED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MEDICATION ", " MED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MEDICATIONS ", " MED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MEDICINE ", " MED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MEDICINES ", " MED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MICROELECTRONIC ", " MICROELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MICROELECTRONICS ", " MICROELECTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MICROSYST ", " MICROSYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MICROSYSTEM ", " MICROSYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MICROSYSTEMS ", " MICROSYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MNG ", " MINING ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MOLECULAR ", " MOLEC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MOLECULARS ", " MOLEC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MOLECULE ", " MOLEC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NANOTECHNOL ", " NANOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NANOTECHNOLOGIES ", " NANOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NANOTECHNOLOGY ", " NANOTECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NATIONAL ", " NATL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NATIONALS ", " NATL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NAVIGATION ", " NAVIGAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NETWORKS ", " NETWORK ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NEUROSCIENCE ", " NEUROSCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NTWK ", " NETWORK ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NTWRK ", " NETWORK ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" NUTRITION ", " NUTR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ONCOLOGY ", " ONCOL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ONCOLYTICS ", " ONCOL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" OPTICS ", " OPTIC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ORTHOPAEDIC ", " ORTHOPAED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ORTHOPAEDICS ", " ORTHOPAED ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PETROCHEMICAL ", " PETROCHEM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PETROCHEMICALS ", " PETROCHEM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PETROLEUM ", " PETR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMA ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUT ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUTIC ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUTICAL ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUTICALS ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUTICL ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUTICLS ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACEUTICS ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACTCL ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHARMACTLS ", " PHARM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHOTONIC ", " PHOTON ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHOTONICS ", " PHOTON ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PHYSICS ", " PHYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRD ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRDUS ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRODS ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRODUCT ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRODUCTION ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRODUCTIONS ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PRODUCTS ", " PROD ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" PUB LTD CO ", " PLC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" R & D ", " RES & DEV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" RECH ", " RES ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" RESEARCH ", " RES ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" RESEARCHES ", " RES ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" RESH ", " RES ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" RESOURCE ", " RESORC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" RESOURCES ", " RESORC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SCIENCE ", " SCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SCIENCES ", " SCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SCIENTIFIC ", " SCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SCIENTIFICS ", " SCI ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SECURE ", " SECUR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SECURITIES ", " SECUR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SECURITY ", " SECUR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SEMICNDCTR ", " SEMICOND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SEMICON ", " SEMICOND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SEMICONDUCTOR ", " SEMICOND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SEMICONDUCTORS ", " SEMICOND ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SERVICE ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SERVICES ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SFTWRS ", " SFTWR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOFTWARE ", " SFTWR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOFTWARES ", " SFTWR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOFTWR ", " SFTWR ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLNS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLTN ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLTNS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLUTINS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLUTION ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLUTIONS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLUTNS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SOLUTS ", " SOLUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SRVCS ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SRVS ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SURGERIES ", " SURG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SURGERY ", " SURG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SURGICAL ", " SURG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SVC ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SVCS ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SVSCS ", " SERV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SYST ", " SYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SYSTEM ", " SYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SYSTEMS ", " SYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" SYSTS ", " SYS ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHN ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNLG ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNLGS ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNOL ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNOLGIES ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNOLOGIES ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNOLOGY ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHNS ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TECHS ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TEKNOLOGIES ", " TECH ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELECOMM ", " TELECOM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELECOMMUN ", " TELECOM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELECOMMUNICATION ", " TELECOM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELECOMMUNICATIONS ", " TELECOM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELECOMMUNICATNS ", " TELECOM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELEKOM ", " TELECOM ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELEPHONE ", " TEL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TELEPHONES ", " TEL ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" THERAPEUTC ", " THERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" THERAPEUTCS ", " THERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" THERAPEUTIC ", " THERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" THERAPEUTICS ", " THERAPEUT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TOB ", " TOBACCO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TRANSPORTATION ", " TRANSPORTAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TRANSPORTATIONS ", " TRANSPORTAT ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNITED STATES ", " US ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNIVERSITIES ", " UNIV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNIVERSITY ", " UNIV ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" UNLIMITED ", " UNLTD ")

    df[cleaned_cols] = df[cleaned_cols].str.strip()  # trim
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\s+', ' ', regex=True)  # itrim
    df[cleaned_cols] = ' ' + df[cleaned_cols] + ' '

    # Step 9: Add my collections
    for j in ("CORP", "INC", "CO", "LTD", "LLC"):
        df[cleaned_cols] = df[cleaned_cols].str.replace(f" {j} (AL|AK|AZ|AR|CA|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|MN|MS|MO|MT|NE|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|VT|VA|WA|WV|WI|WY|DC|CA|WA|WIS|UTAH|NE|OHIO|NY|TENN|ORE|MASS|COLO|NJ|TX|VA|DEL|DE|DELAWARE) $", f" {j} ", regex=True)

    # Step 10: END
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"(\b\S)+(\s)+(&)+(\s)+(\S\b)", r"\1&\5", regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"(\b\S)+(\s)+(\S&)", r"\1\3", regex = True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(r"\b(\S)\s+(?=\S\b)", r"\1", regex = True)

    df[cleaned_cols] = df[cleaned_cols].str.replace(" CROP ", " CORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" OFTHE ", " OF THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(r'\bTHEE\b(?!\s+CORP)', 'THE', regex=True)
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TEH ", " THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCC ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CONPANY ", " CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ANDCO ", " AND CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COPR ", " CORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORPORATION) ", " CORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORPORATION] ", " CORP ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" FORTHE ", " FOR THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COMPANYTHE ", " CO THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" COTHE ", " CO THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INTHE ", " IN THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" ONTHE ", " ON THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" INCORPORATED) ", " INC ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" LTDTHE ", " LTD THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TOTHE ", " TO THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" BYTHE ", " BY THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" CORPORATIONTHE ", " CORP THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" FROMTHE ", " FROM THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" MANUFACTURING ", " MFG ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" OFCO ", " OF CO ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" THTE ", " THE ")
    df[cleaned_cols] = df[cleaned_cols].str.replace(" TTHE ", " THE ")

    return df

def clean_file(file_path:str,
               output_path:str,
               name_cols: str | list,
               cleaned_cols:str|list,
               filter:Callable=None,
               usecols=None) -> None:
    if file_path[-4:] == ".csv":
        reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, iterator=True, dtype=str, usecols=usecols)
    elif file_path[-4:] == ".dta":
        reader = pd.read_stata(file_path, chunksize=CHUNK_SIZE, iterator=True, columns=usecols)
    else:
        #raise Exception("file format not supported")
        print("file format not supported")
        return

    total_rows, filtered_rows = 0, 0
    first = True
    print("clean_file: 开始处理文件：" + file_path)
    for i, chunk in enumerate(reader):
        total_rows += len(chunk)
        if filter is None:
            chunk = chunk.fillna('')
        else:
            chunk = chunk[filter(chunk.fillna(''))]
        filtered_rows += len(chunk)

        chunk = standardize_company_names(chunk, name_cols, cleaned_cols)
        chunk.to_csv(
            output_path,
            mode='w' if first else 'a',
            header=first,
            index=False,
        )
        first = False

        print(f"已处理批次 {i + 1}, 累计行数: {total_rows:,}, 累计输出行数: {filtered_rows:,}")

def match_ciq_gvkey(file_path:str, ciq_gvkey_path:str, output_path:str) -> None:
    ciq_gvkey_df=pd.read_stata(ciq_gvkey_path,columns=['companyid','gvkey'])
    ciq_to_gvkey = dict(zip(ciq_gvkey_df['companyid'], ciq_gvkey_df['gvkey']))

    reader = pd.read_csv(file_path, chunksize=CHUNK_SIZE, iterator=True, dtype=str, usecols=['companyid','companyname','ultimateparentcompanyid', 'companyname_std'])

    def _match_rule(row):
        cid,pid=row["_tmp_cid"],row["_tmp_pid"]
        if pd.notna(cid) and pd.notna(pid):
            row["matched_key"]=f"{cid}|{pid}"
            row["type"]="gvkey"
        elif pd.notna(cid):
            row["matched_key"] = cid
            row["type"]="gvkey"
        elif pd.notna(pid):
            row["matched_key"] = pid
            row["type"] = "gvkey"
        else:
            row["matched_key"]=row["companyid"]
            row["type"]="companyid"
        return row

    total_rows, filtered_rows = 0, 0
    first = True
    print("match_ciq_gvkey: 开始处理文件：" + file_path)
    for i, chunk in enumerate(reader):
        total_rows += len(chunk)

        chunk["_tmp_cid"] = chunk['companyid'].map(ciq_to_gvkey)
        chunk["_tmp_pid"] = chunk['ultimateparentcompanyid'].map(ciq_to_gvkey)
        chunk=chunk.apply(_match_rule, axis=1)
        chunk.drop(["_tmp_cid", "_tmp_pid"], axis=1, inplace=True)
        chunk.to_csv(
            output_path,
            mode='w' if first else 'a',
            header=first,
            index=False,
        )
        first = False

        filtered_rows += len(chunk)
        print(f"已处理批次 {i + 1}, 累计行数: {total_rows:,}, 累计输出行数: {filtered_rows:,}")

def standarlize_main(type,file_path:str,output_path:str) -> None:
    if type=="OWNER":
        clean_file(
            file_path,
            output_path,
            'own_name',
            'name_std',
            lambda df: ((df['own_nalty_country_cd'].isin(country_code)) | (df['own_nalty_state_cd'].isin(state_code)) | (
                df['own_addr_state_cd'].isin(state_code)))
        )
    elif type=="ASSIGNEE":
        clean_file(
            file_path,
            output_path,
            'ee_name',
            'name_std',
            lambda df: ((df['ee_country'].isin(country_name))|(df['ee_natlty'].isin(state_name))|(df['ee_state'].isin(state_name)))
        )
    elif type=="ASSIGNOR":
        clean_file(
            file_path,
            output_path,
            'or_name',
            'name_std',
            lambda df: ((df['or_country'].isin(country_name)) | (df['or_natlty'].isin(state_name)) | (df['or_state'].isin(state_name)))
        )
    elif type=="CRSP":
        clean_file(
            file_path,
            output_path,
            "comnam",
            'name_std',
        )
    elif type=="COMPUSTAT":
        clean_file(
            file_path,
            output_path,
            "conm",
            'name_std',
        )
    elif type=="CIQ":
        clean_file(
            file_path,
            output_path,
            'companyname',
            'name_std',
        )
    elif type=="WRDS":
        clean_file(
            file_path,
            output_path,
            "clean_company",
            'name_std',
            lambda df: (df['gvkey'] != '000000')
        )
    else:
        raise ValueError("unknown type: "+type)