import argparse, psycopg2, boto3, re, pandas, os, pycountry, zipfile, io
from this import d
from calendar import c
import xml.etree.ElementTree as ET
import numpy as np

from psycopg2 import extras

def check_sequencing_file_on_S3(samplename, library_name, bucket, prefix, client):
    objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix+library_name+"_")

    if objects["KeyCount"]==0:
        #Fix for some Timor-Leste misnamed files
        if library_name[:2]=="TL":
            objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix+library_name[:2]+"-" + library_name[2:] +"_")
        #Fix for FIND samples sequenced at OSR
        elif re.match(r'^TB[0-9]{16}', samplename):
            objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix+"/Simone Battaglia - " + library_name + "_")
        else:
            objects = client.list_objects_v2(Bucket=bucket, Prefix=prefix+library_name+".")        
    try:
        if not objects["KeyCount"]%2:
            return([ (re.match(r'(.*?)(?:_S[0-9]+)?(?:_L?001)?(?:[_\.]R?(?:[12]))?(?:_L?001)?\.(?:fastq|fq)\.gz$', x["Key"].rsplit("/", 1)[-1]).group(1), bucket+"/"+x["Key"], {tag["Key"]: tag["Value"] for tag in client.get_object_tagging(Bucket=bucket, Key=x["Key"])["TagSet"]}.get("md5sum")) for x in objects["Contents"]])
        else:
            return([])
    except KeyError:
        return([])

def main(arguments):

    global sequencing_key
    s3_client_data = boto3.client("s3", boto3.client("s3").get_bucket_location(Bucket=arguments.data_file_bucket)['LocationConstraint'])

    obj=s3_client_data.get_object(Bucket=arguments.data_file_bucket, Key=os.environ["FILE_NAME"])

    metadata = {}

    rds_client = boto3.client('rds', region_name=arguments.db_aws_region)
    token = rds_client.generate_db_auth_token(DBHostname=arguments.db_host, Port=arguments.db_port, DBUsername=arguments.db_user)
    conn = psycopg2.connect(host=arguments.db_host, port=arguments.db_port, database=arguments.db_name, user=arguments.db_user, password=token)
    
    curr = conn.cursor()

    if arguments.data_format=="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        workbook = obj["Body"].read()
        xl = pandas.ExcelFile(workbook)
        try:
            #Reading metadata in the custom properties field of the Workbook.
            custom_properties = ET.fromstring(zipfile.ZipFile(io.BytesIO(workbook)).read("docProps/custom.xml"))
            for child in list(custom_properties):
                value = list(child)[0].text.strip()
                if value.lower() == "true":
                    metadata[child.get("name")] = True
                elif value.lower() == "false":
                    metadata[child.get("name")] = False
                else:
                    metadata[child.get("name")] = list(child)[0].text
        except KeyError:
            pass

    data_dict = {}

    srr_ids = []
    srs_ids = []
    if "general" in xl.sheet_names:
        data_dict["general"] = pandas.read_excel(workbook, sheet_name="general", header=0, dtype={"Strain ID": str, "LABNO": str})
    for sheet in ["pDST", "MIC"]:
        if sheet in xl.sheet_names:
            data_dict[sheet] = pandas.read_excel(workbook, sheet_name=sheet, header=0, dtype={"Strain ID": str, "LABNO": str})
            srr_ids.extend(data_dict[sheet].loc[data_dict[sheet]["Strain ID"].str.fullmatch(r'[ESD]RR[0-9]+'), "Strain ID"].values.tolist())
            srs_ids.extend(data_dict[sheet].loc[data_dict[sheet]["Strain ID"].str.fullmatch(r'[ESD]RS[0-9]+'), "Strain ID"].values.tolist())
    countries = pandas.DataFrame([(x.alpha_2, x.alpha_3, int(x.numeric)) for x in list(pycountry.countries)], columns=("alpha2", "alpha3", "country_code"))
    if srr_ids:
        curr.execute(""" 
            SELECT sample_name, library_name
            FROM sequencing_data
            NATURAL INNER JOIN sample
            WHERE sequencing_data.library_name IN %s
            ;""", (tuple(list(set(srr_ids))),)
        )
        srr_matches = pandas.DataFrame(curr.fetchall(), columns=["SampleName", "Strain ID"])
        for sheet in ["pDST", "MIC"]:
            if sheet in data_dict.keys():
                data_dict[sheet]["Strain ID"] = data_dict[sheet].merge(srr_matches, how="left")["SampleName"].combine_first(data_dict[sheet]["Strain ID"])
    if srs_ids: 
        curr.execute(""" 
            SELECT sample_name, sra_name
            FROM sample
            WHERE sra_name IN %s
            ;""", (tuple(list(set(srs_ids))),)
        )
        srs_matches = pandas.DataFrame(curr.fetchall(), columns=["SampleName", "Strain ID"])
        for sheet in ["pDST", "MIC"]:
            if sheet in data_dict.keys():
                data_dict[sheet]["Strain ID"] = data_dict[sheet].merge(srs_matches, how="left")["SampleName"].combine_first(data_dict[sheet]["Strain ID"])
    for sheet in ["pDST", "MIC"]:
        if sheet in data_dict.keys():
            if not data_dict[sheet][list(set(list(data_dict[sheet])).intersection(set(["Sampling date", "Year of isolation", "LABNO", "Sample type", "Country origin"])))].empty:
                data_dict["general"] = data_dict[sheet][list(set(list(data_dict[sheet])).intersection(set(["Strain ID", "Sampling date", "Year of isolation", "LABNO", "Sample type", "Country origin"])))].copy().drop_duplicates()

    if "general" in data_dict.keys():
        data = data_dict["general"]
        
        if "submission_date" not in metadata.keys():
            data["submission_date"] = arguments.submission_date
        else: 
            data["submission_date"] = metadata["submission_date"]

        if metadata.get("fastq_bucket_name", ""):
            s3_client_fastq = boto3.client("s3", boto3.client("s3").get_bucket_location(Bucket=metadata["fastq_bucket_name"])['LocationConstraint'])

        if "Sampling date" in list(data):
            data['Sampling date'] = data['Sampling date'].replace('', np.nan).fillna(data.groupby('Strain ID')['Sampling date'].transform('first'))
            data["sampling_date"] = "["+data["Sampling date"].astype(str)+","+data["Sampling date"].astype(str)+"]"
        elif "Year of isolation" in list(data):
            data['Year of isolation'] = data['Year of isolation'].replace('', np.nan).fillna(data.groupby('Strain ID')['Year of isolation'].transform('first')).fillna(0).astype(int)
            data["sampling_date"] = "["+data["Year of isolation"].astype(str)+"-01-01,"+data["Year of isolation"].astype(str)+"-12-31]"
        else:
            data["sampling_date"] = ""

        if "LABNO" not in list(data):
            data["LABNO"] = data["Strain ID"]

        if "Sample type" not in list(data):
            data["Sample type"] = ""
        else: 
            data['Sample type'] = data['Sample type'].replace('', np.nan).fillna(data.groupby('Strain ID')['Sample type'].transform('first'))

        if "Country origin" in list(data):
            data['Country origin'] = data['Country origin'].replace('', np.nan).fillna(data.groupby('Strain ID')['Country origin'].transform('first'))
            data["Country origin"] = data["Country origin"].str.upper()
            if data["Country origin"].str.match(r'^[A-Z]{2}$').all():
                data = data.merge(countries, how="left", left_on="Country origin", right_on="alpha2")
            elif data["Country origin"].str.match(r'^[A-Z]{3}$').all():
                data = data.merge(countries, how="left", left_on="Country origin", right_on="alpha3")
            elif data["Country origin"].str.match(r'^[0-9]{3}').all():
                data["country_code"] = data["Country origin"].astype(int)
        else:
            data["country_code"] = 0

        sample_data = data[["Strain ID", "LABNO", "country_code", "sampling_date", "submission_date", "Sample type"]].drop_duplicates()

        prefix = metadata.get("prefix", "").rstrip("/")

        if prefix:
            prefix = prefix + "/"
        
        sequencing_data = []
        md5sums = {}
        if "fastq_bucket_name" in metadata.keys():
            for i, row in sample_data.iterrows():
                if not re.match(r"SAM(?:EA|N)[0-9]+", row["Strain ID"]):
                    for j in check_sequencing_file_on_S3(row["Strain ID"], row["LABNO"].strip(), metadata["fastq_bucket_name"], prefix, s3_client_fastq):
                        md5sums.setdefault(row["Strain ID"], []).append(j[2])
                        if row["Strain ID"] != j[0]:
                            sequencing_data.append([row["Strain ID"], row["Strain ID"] +"_"+ j[0], "S3", "WGS", "ILLUMINA", "PAIRED", j[1], j[2]])
                        else:
                            sequencing_data.append([row["Strain ID"], row["Strain ID"], "S3", "WGS", "ILLUMINA", "PAIRED", j[1], j[2]])

        replacement = {}
        for md5s in md5sums:
            curr.execute("""
                SELECT sample_name, array_agg(sequencing_data_hash.value)
                FROM sample
                NATURAL INNER JOIN sequencing_data
                NATURAL INNER JOIN sequencing_data_hash
                WHERE sequencing_data_hash.value IN %s
                GROUP BY sample_name
            ;""", (tuple(md5sums[md5s]),))

            matches = curr.fetchall()
            
            if matches and len(matches[0][1])==2:
                replacement[md5s] = matches[0][0]

        print(replacement)

        print(len(replacement))
 
        extras.execute_values(curr, """
            INSERT INTO sample(sample_name, country_id, ncbi_taxon_id, sampling_date, submission_date, isolation_source)
            SELECT
                sample_name,
                NULLIF(country_id, 0)::int,
                1773,
                NULLIF(NULLIF(NULLIF(sampling_date, ''), '[NaT,NaT]'), '[0-01-01,0-12-31]')::daterange,
                submission_date::date,
                NULLIF(isolation_source, '')
            FROM (
                VALUES %s
            ) entry(sample_name, country_id, sampling_date, submission_date, isolation_source)
            ON CONFLICT (sample_name) DO
            UPDATE SET
                country_id = COALESCE(EXCLUDED.country_id, sample.country_id),
                sampling_date = COALESCE(EXCLUDED.sampling_date, sample.sampling_date),
                isolation_source = COALESCE(EXCLUDED.isolation_source, sample.isolation_source);
        """, sample_data[["Strain ID", "country_code", "sampling_date", "submission_date", "Sample type"]].drop_duplicates().replace({"Strain ID": replacement}).values.tolist())

        if replacement:
            extras.execute_values(curr, """
                INSERT INTO "additional_sample_name"("sample_id", "db", "db_label", "sample_name_synonym")
                SELECT "sample"."sample_id",
                    'Additional sequencing name',
                    'Sample name',
                    entry."AddName"
                FROM (
                    VALUES %s
                    ) entry("SampleName", "AddName")
                INNER JOIN "sample" ON "sample"."sample_name"=entry."SampleName"
                ON CONFLICT DO NOTHING;
                """, [[value, key] for key, value in replacement.items()]
                )

        if sequencing_data: 
            seq = pandas.DataFrame(sequencing_data)

            seq = seq[~seq[0].isin([x for x in replacement.keys()])]

            if not seq.empty:

                extras.execute_values(curr, """
                    INSERT INTO "sequencing_data"("sample_id", "library_name", "data_location", "library_preparation_strategy", "sequencing_platform", "library_layout", "file_path")
                    SELECT 
                        sample.sample_id,
                        entry.library_name,
                        entry.data_location,
                        entry.library_preparation_strategy,
                        entry.sequencing_platform,
                        entry.library_layout,
                        entry.file_path
                    FROM (
                        VALUES %s
                    ) entry (sample_name, library_name, data_location, library_preparation_strategy, sequencing_platform, library_layout, file_path)
                    INNER JOIN sample on sample.sample_name=entry.sample_name
                    ON CONFLICT DO NOTHING;
                    """, seq[[0,1,2,3,4,5,6]].values.tolist())

                extras.execute_values(curr, """
                    INSERT INTO "sequencing_data_hash"(sequencing_data_id, algorithm, value)
                    SELECT 
                        sequencing_data_id, 
                        'md5', 
                        entry.md5sum
                    FROM (
                        VALUES %s
                    ) entry (file_path, md5sum)
                    INNER JOIN sequencing_data on sequencing_data.file_path=entry.file_path
                    ON CONFLICT DO NOTHING;
                    """, seq[[6,7]].values.tolist())

    if "dataset" in metadata.keys():
        curr.execute("""
            INSERT INTO dataset(dataset_name, dataset_origin, dataset_owner, contact_email, submission_date)
            VALUES (%s, 'Dataset imported from tabular file (' || %s || ') data stored on S3', %s, %s, %s)
            ON CONFLICT (dataset_name) DO NOTHING
            ;""", (metadata["dataset"], os.environ["FILE_NAME"], metadata.get("contributor1", None), metadata.get("contributor1_email", None), arguments.submission_date)
        )

        for contributor in list(set([x.split("_")[0] for x in metadata.keys() if (x.startswith("contributor") or x.startswith("collaborator"))])):

            curr.execute("""
                INSERT INTO contributor(contributor_name, contributor_affiliation, contributor_email)
                VALUES (%s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (metadata[contributor], metadata.get(contributor+"_affiliation", None), metadata.get(contributor+"_email", None))
            )

            curr.execute("""
                INSERT INTO dataset_to_contributor(dataset_id, contributor_id)
                SELECT dataset.dataset_id,
                    contributor.contributor_id
                FROM (
                    VALUES (%s, %s)
                ) entry(dataset_name, contributor_name)
                NATURAL INNER JOIN dataset
                NATURAL INNER JOIN contributor
                ON CONFLICT DO NOTHING
            """, (metadata["dataset"], metadata[contributor])
            )
    
        try:
            dataset_to_sample = sample_data[["Strain ID"]].replace({"Strain ID": replacement}).drop_duplicates()
        except UnboundLocalError:
            dataset_to_sample = data_dict["pDST"][["Strain ID"]].replace({"Strain ID": replacement}).drop_duplicates()
        dataset_to_sample["dataset"] = metadata["dataset"]
        extras.execute_values(curr, """
            INSERT INTO dataset_to_sample(dataset_id, sample_id)
            SELECT
                dataset.dataset_id,
                sample.sample_id
            FROM (
                VALUES %s
            ) entry(sample_name, dataset_name)
            NATURAL INNER JOIN sample
            INNER JOIN dataset on dataset.dataset_name=entry.dataset_name
            ON CONFLICT DO NOTHING;
            """, dataset_to_sample.values.tolist()
        )
            
    decimal_regexp = r'(\d+[.,]?\d*|[.,]\d+)'

    if "pDST" in data_dict.keys():
        final = []

        data_dict["pDST"] = data_dict["pDST"].replace({"Strain ID": replacement})

        if "Method LJ" not in list(data_dict["pDST"]):
            data_dict["pDST"]["Method LJ"] = ""

        column_list = ["Strain ID", "DST Method", "Method LJ"]

        if "Drug" in list(data_dict["pDST"]) and "Result" in list(data_dict["pDST"]):
                final = data_dict["pDST"][data_dict["pDST"]["Result"].isin(["R", "S", "I"])].fillna("")[ column_list + ["Drug", "Concentration", "Result"]].values.tolist()
        else:
            if  [x for x in list(data_dict["pDST"]) if '(CC)' in x]:
            
                tmp = data_dict["pDST"][ column_list + [x for x in list(data_dict["pDST"]) if '(CC)' in x]].melt(id_vars=column_list, value_vars=[x for x in list(data_dict["pDST"]) if '(CC)' in x], var_name="Drug").dropna()

                tmp["value"] = tmp["value"].str.replace(" ", "")

            
                tmp = tmp[tmp["value"].str.match(r'[RSI]\('+decimal_regexp+'\)')]

                tmp[["Result", "Concentration"]] = tmp["value"].str.extract(r'([RIS])\('+decimal_regexp+'\)').fillna("")

                tmp["Concentration"] = tmp["Concentration"].str.replace(",", ".")

                final.append(tmp[column_list + ["Drug", "Concentration", "Result"]].values.tolist())

            if [x for x in list(data_dict["pDST"]) if re.match(r'[A-Z]{3,4} \(?' + decimal_regexp+r'(?: [muμ]g/L)?\)?', x.strip())]:
            
                tmp = data_dict["pDST"][ column_list + [x for x in list(data_dict["pDST"]) if re.match(r'[A-Z]{3,4} \(?' + decimal_regexp+r'\)?', x.strip())]].melt(id_vars=column_list, value_vars=[x for x in list(data_dict["pDST"]) if re.match(r'[A-Z]{3,4} \(?' + decimal_regexp+r'\)?', x.strip())], var_name="DrugTmp").dropna()

                tmp["Result"] = tmp["value"].str.strip()
                tmp = tmp[tmp["Result"].isin(["R", "S", "I"])]

                tmp[["Drug", "Concentration"]] = tmp["DrugTmp"].str.extract(r'([A-Z]{3,4}) \(?'+decimal_regexp+r'\)?')

                tmp["Concentration"] = tmp["Concentration"].str.replace(",", ".")

                final.extend(tmp[  column_list + ["Drug", "Concentration", "Result"]].values.tolist())

            if [x for x in list(data_dict["pDST"]) if re.match(r'^[A-Z]{3,4}$', x.strip())]:
            
                tmp = data_dict["pDST"][column_list + [x for x in list(data_dict["pDST"]) if re.match(r'^[A-Z]{3,4}$', x.strip())]].melt(id_vars=column_list, value_vars=[x for x in list(data_dict["pDST"]) if re.match(r'^[A-Z]{3,4}$', x.strip())], var_name="DrugTmp").dropna()

                tmp["Result"] = tmp["value"].str.strip()
                tmp = tmp[tmp["Result"].isin(["R", "S", "I"])]

                tmp[["Drug"]] = tmp["DrugTmp"].str.extract(r'([A-Z]{3,4})')

                tmp["Concentration"] = ""

                final.extend(tmp[column_list + ["Drug", "Concentration", "Result"]].values.tolist())

        if final:

            pDST_data = pandas.DataFrame(final, columns=column_list +  ["Drug", "Concentration", "Result"])

            pDST_data["Drug"] = pDST_data["Drug"].str.replace(" ", "").str.replace("(CC)", "", regex=False)

            for col in column_list:
                pDST_data[col] = pDST_data[col].str.replace(" ", "")

            pDST_data["submission_date"] = arguments.submission_date

            pDST_data["Concentration"] = pDST_data["Concentration"].astype(str)

            extras.execute_values(curr,"""
                DELETE FROM "phenotypic_drug_susceptibility_test"
                WHERE test_id IN (
                    SELECT test_id
                    FROM phenotypic_drug_susceptibility_test pdst
                    INNER JOIN sample on pdst.sample_id=sample.sample_id
                    LEFT JOIN genphen_growthmedium on pdst.medium_id=genphen_growthmedium.medium_id
                    LEFT JOIN gephen_pdsassessmentmethod pdsam ON pdsam.method_id=pdst.method_id
                    LEFT JOIN genphen_drug ON genphen_drug.drug_id=pdst.drug_id
                    LEFT JOIN genphen_drugsynonym ON genphen_drugsynonym.drug_id=pdst.drug_id
                    INNER JOIN (
                        VALUES %s
                    ) entry(sample_name, drug, medium, method, concentration, result)
                        ON entry.sample_name=sample.sample_name
                        AND entry.result = pdst.test_result
                        AND (entry.drug = genphen_drugsynonym.drug_name_synonym OR entry.drug = genphen_drug.drug_name)
                        AND (NULLIF(entry.concentration, '')::float=pdst.concentration OR (entry.concentration='' AND pdst.concentration IS NULL))
                        AND (entry.method=pdsam.method_name OR (entry.method='' AND pdsam.method_name IS NULL))
                        AND (entry.medium=genphen_growthmedium.medium_name OR (entry.medium='' AND genphen_growthmedium.medium_name IS NULL))
                );""", pDST_data[["Strain ID", "Drug", "DST Method", "Method LJ", "Concentration", "Result"]].values.tolist())

            extras.execute_values(curr, """
                INSERT INTO phenotypic_drug_susceptibility_test(
                    sample_id,
                    drug_id,
                    medium_id,
                    method_id,
                    concentration,
                    test_result,
                    submission_date)
                SELECT "sample"."sample_id",
                    coalesce("genphen_drug"."drug_id", genphen_drugsynonym.drug_id),
                    "genphen_growthmedium"."medium_id",
                    pdsam.method_id,
                    NULLIF(entry."concentration", '')::float,
                    entry."result",
                    entry.submission_date::date
                FROM (
                    VALUES %s
                ) entry(sample_name, drug, medium, method, concentration, result, submission_date)
                INNER JOIN "sample" ON "sample"."sample_name"=entry."sample_name"
                LEFT JOIN "genphen_drug" ON "genphen_drug"."drug_name"=entry."drug"
                LEFT JOIN genphen_drugsynonym ON genphen_drugsynonym.drug_name_synonym=entry.drug
                LEFT JOIN "genphen_growthmedium" ON "genphen_growthmedium"."medium_name"=entry."medium"
                LEFT JOIN gephen_pdsassessmentmethod pdsam ON pdsam.method_name=entry.method;
                """, pDST_data[["Strain ID", "Drug", "DST Method", "Method LJ", "Concentration", "Result", "submission_date"]].values.tolist()
                )

    if "MIC" in data_dict.keys():
        mic = data_dict["MIC"].replace({"Strain ID": replacement})

        if "Drug" in list(mic) and "Value" in list(mic):

            mic_data = mic.copy()
            
            mic_data["value"] = mic_data["Value"].astype(str).str.replace(" ", "")
            
        else:
            mic_data = mic[["Strain ID", "Plate"] + [x for x in list(mic) if re.match(r'[A-Z]{3,4}', x.strip())]].melt(id_vars=["Strain ID", "Plate"], value_vars=[x for x in list(mic) if re.match(r'[A-Z]{3,4}', x.strip())], var_name="Drug").dropna()

            mic_data["value"] = mic_data["value"].astype(str).str.replace(" ", "")

        if metadata.get("infer_concentration_range", False):

            concentration_range = mic_data[["Plate", "Drug", "value"]].copy()

            concentration_range["value"] = concentration_range["value"].str.replace(r'[≤≥<>=]+', '', regex=True).str.replace(",", ".", regex=False).astype(float)

            concentration_range=concentration_range.drop_duplicates().sort_values(by=["Plate", "Drug", "value"])
        
            concentration_range["lag"] = concentration_range.groupby(["Plate", "Drug"])["value"].shift(1).fillna(0)

            concentration_range["value"] = concentration_range["value"].astype("object")

            concentration_range = concentration_range.reset_index().set_index(["Plate", "Drug", "value"])
            
        decimal_regexp = r'(\d+[.,]?\d*|[.,]\d+)'


        for i, row in mic_data.iterrows():

            #If the value is already in the form of a numeric range (i.e. (2.5, 5]), we (almost) don't need to do anything
            #We also allow for something very unnatural like "(2,5 , 5,0]"...
            range_match = re.match(r'(\(|\[)'+decimal_regexp+r','+decimal_regexp+r'(\)|\])', row["value"])
            if range_match:
                mic_data.loc[i, "MICValue"] = range_match.group(1) + range_match.group(2).replace(",", ".") + "," + range_match.group(3).replace(",", ".") + range_match.group(4)
            #If we joined the lower concentration bound using the data we have on microdilution plates plates, then the output is straightforward (it's "(lower_bound, upper_bound]"::varchar)
            else:
                #Otherwise if the value is a simple number and we don't know the smaller concentration tested, we have no other choices than creating ("[value, value]"::varchar)
                try:
                    val = float(row["value"].replace(",", "."))
                    if metadata.get("infer_concentration_range", False):
                        mic_data.loc[i, "MICValue"] = "("+str(concentration_range.loc[(row["Plate"], row["Drug"], val)]["lag"]) + ","+str(val)+"]"
                    else:
                        mic_data.loc[i, "MICValue"] = "(" + str(val/2) + "," + str(val)+ "]"
                except ValueError:
                    #We can also capture "(equal or) smaller/greater than" signs and create a range
                    sign_match = re.match(r'(≤|≥|[<>]=?)'+ decimal_regexp, row["value"])
                    if sign_match:
                        operator = sign_match.group(1)
                        value = str(float(sign_match.group(2).replace(",",".")))
                        if operator in ["<=", "≤"]:
                            mic_data.loc[i, "MICValue"] = "(0," + value + "]"
                        elif operator == "<":
                            mic_data.loc[i, "MICValue"] = "(0," + value + ")"
                        elif operator in [">=", "≥"]:
                            mic_data.loc[i, "MICValue"] = "[" + value + ",)"
                        elif operator == ">":
                            mic_data.loc[i, "MICValue"] = "(" + value + ",)"
                    #Otherwise we capture other strings and format them into a proper range
                    #For instance "2.5-5", "2.5or5", "2.5|5"
                    else:
                        other_ranges_match = re.match(decimal_regexp+r'(?:or|/|-)'+decimal_regexp, row["value"])
                        if other_ranges_match:
                            mic_data.loc[i, "MICValue"] = "("+other_ranges_match.group(1).replace(",", ".")+","+other_ranges_match.group(2).replace(",", ".")+"]"                

        mic_data["submission_date"] = arguments.submission_date

        extras.execute_values(curr,"""
            DELETE FROM "minimum_inhibitory_concentration_test"
            WHERE test_id IN (
                SELECT test_id
                FROM minimum_inhibitory_concentration_test mict
                INNER JOIN sample on mict.sample_id=sample.sample_id
                LEFT JOIN genphen_drug ON genphen_drug.drug_id=mict.drug_id
                LEFT JOIN genphen_drugsynonym ON genphen_drugsynonym.drug_id=mict.drug_id
                INNER JOIN (
                    VALUES %s
                ) entry(sample_name, drug, plate, value)
                    ON entry.sample_name=sample.sample_name
                    AND (entry.drug = genphen_drugsynonym.drug_name_synonym OR entry.drug = genphen_drug.drug_name)
                    AND entry.plate = mict.plate
                    AND entry.value::numrange = mict.mic_value
            );""", mic_data[["Strain ID", "Drug", "Plate", "MICValue"]].values.tolist())

        extras.execute_values(curr,"""
            INSERT INTO "minimum_inhibitory_concentration_test"(sample_id, drug_id, plate, mic_value, submission_date)
            SELECT sample.sample_id,
                    coalesce(genphen_drug.drug_id, genphen_drugsynonym.drug_id),
                    entry.plate,
                    entry.value::numrange,
                    entry.submission_date::date
            FROM (
                VALUES %s
            ) entry(sample_name, drug, plate, value, submission_date)
            INNER JOIN sample on sample.sample_name=entry.sample_name
            LEFT JOIN "genphen_drug" ON "genphen_drug"."drug_name"=entry."drug"
            LEFT JOIN genphen_drugsynonym ON genphen_drugsynonym.drug_name_synonym=entry.drug
            ;""", mic_data[["Strain ID", "Drug", "Plate", "MICValue", "submission_date"]].values.tolist())

    conn.commit()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument("--db_host", help="AWS RDS database endpoint")
    parser.add_argument("--db_name", help="Database name")
    parser.add_argument("--db_user", help="Database user name (with AWS RDS IAM authentication)")
    parser.add_argument("--db_port", help="Database port")
    parser.add_argument("--db_aws_region", help="Database AWS region location")
    parser.add_argument("--data_file_bucket", help="Bucket where the data file is located")
    parser.add_argument("--submission_date")
    parser.add_argument("--data_format")
    
    args = parser.parse_args()

    main(args)