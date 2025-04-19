import pandas as pd
from datetime import datetime, timedelta

# Fields which must be filled out in all OK/AAD cases in IA/Participant
ALL_IA_PARTICIPANT_REQUIRED_FIELDS = ["METRICID", "PRIVACYACKNOWLEDGEMENT",
                                      "PRIVACYTIMESTAMP", "KNOWNUMBERS", "KNOWNUMBERSINFO", "AGE",
                                      "SEXATBIRTH", "OTHERSTATINS", "OTHERSTATINSINFO", "CYCLOSPORINE",
                                      "CYCLOSPORINEINFO", "WARFARIN", "WARFARININFO", "HEARTATTACK", "STROKE",
                                      "PROCEDURE", "PAD", "CARDIACINFO", "TRIGLYCERIDES", "TRIGLYCERIDESINFO",
                                      "TOTALCHOLESTEROL", "LDLCHOLESTEROL",
                                      "HDLCHOLESTEROL", "CHOLESTEROLINFO",
                                      "ASCVDRISKSCORE", "LIVERDISEASE", "LIVERDISEASEINFO", "KIDNEYDISEASE",
                                      "KIDNEYDISEASEINFO", "ALCOHOLUSAGE", "ALCOHOLUSAGEINFO",
                                      "CHOLESTEROLMEDSREACTION",
                                      "CHOLESTEROLMEDSREACTIONINFO", "OTHERMEDS"]

IA_ALL_COLUMNS = ["MetricID", "AssessmentStartTimestamp", "AccountCreationTimestamp", "ParticipantID",
                  "ParticipantEmail", "UserEmail", "ClinicianFirstName", "ClinicianLastName", "PrivacyAcknowledgement",
                  "PrivacyTimestamp", "KnowNumbers", "KnowNumbersInfo", "Age", "SexatBirth", "Pregnant", "PregnantInfo",
                  "EthnicityRace", "OtherStatins", "OtherStatinsInfo", "Cyclosporine", "CyclosporineInfo", "Warfarin",
                  "WarfarinInfo", "HeartAttack", "Stroke Procedure", "PAD", "CardiacInfo", "ASCVDFamilyHistoryFather",
                  "ASCVDFamilyHistoryMother", "ASCVDFamilyHistoryInfo", "Triglycerides", "TriglyceridesInfo",
                  "TotalCholesterol", "LDLCholesterol", "HDLCholesterol", "CholesterolInfo", "SystolicBP",
                  "DiastolicBP", "BPMeds", "Diabetes", "Smoker", "ASCVDRiskScore", "LiverDisease", "LiverDiseaseInfo",
                  "KidneyDisease", "KidneyDiseaseInfo", "AlcoholUsage", "AlcoholUsageInfo", "CholesterolMedsReaction",
                  "CholesterolMedsReactionInfo", "OtherMeds", "WaistSize35", "WaistSize40", "WaistSizeInfo",
                  "PsoriasisRA", "HIV", "Preeclampsia", "EarlyMenopause", "HsCRPTested", "CACTested", "HsCRPGT2",
                  "CACGT0", "CAC100orG", "Outcome", "KickoutType", "KickoutTimeStamp", "ConfirmAnswers",
                  "ConfirmationCheckbox", "CheckboxTimestamp", "TalkToADoctor", "ConfirmDoctor", "DoctorTimestamp",
                  "DFLScroll", "DFLDownload", "ContinueToBuy", "StudyTag"]

# Fields which must be filled out in all OK/AAD cases in RA/Participant
ALL_RA_PARTICIPANT_REQUIRED_FIELDS = ["METRICID", "REORDERTIMESTAMP", "PARTICIPANTID",
                                      "PARTICIPANTEMAIL", "PRIVACYACKNOWLEDGEMENT",
                                      "PRIVACYTIMESTAMP", "HEARTATTACK", "STROKE",
                                      "PROCEDURE", "PAD", "CARDIACINFO",
                                      "OTHERSTATINS", "OTHERSTATINSINFO", "CYCLOSPORINE",
                                      "CYCLOSPORINEINFO", "WARFARIN", "WARFARININFO", "LIVERSYMPTOMS",
                                      "MUSCLESYMPTOMS", "MUSCLESYMPTOMSINFO",
                                      "ALCOHOLUSAGE", "ALCOHOLUSAGEINFO", "OTHERMEDS"]

# Fields which must be filled out in all OK/AAD cases in RA/Clinician
ALL_RA_CLINICIAN_REQUIRED_FIELDS = ["METRICID", "USEREMAIL", "CLINICIANFIRSTNAME",
                                    "CLINICIANLASTNAME", "PARTICIPANTID", "PARTICIPANTEMAIL", "PRIVACYACKNOWLEDGEMENT",
                                    "PRIVACYTIMESTAMP", "HEARTATTACK", "STROKE",
                                    "PROCEDURE", "PAD", "CARDIACINFO",
                                    "OTHERSTATINS", "OTHERSTATINSINFO", "CYCLOSPORINE",
                                    "CYCLOSPORINEINFO", "WARFARIN", "WARFARININFO", "LIVERSYMPTOMS",
                                    "MUSCLESYMPTOMS", "MUSCLESYMPTOMSINFO",
                                    "ALCOHOLUSAGE", "ALCOHOLUSAGEINFO", "OTHERMEDS"]

# Fields which must be filled out in all OK/AAD cases in IA/Clinician
ALL_IA_CLINICIAN_REQUIRED_FIELDS = ["METRICID", "PARTICIPANTID", "USEREMAIL", "CLINICIANFIRSTNAME",
                                    "CLINICIANLASTNAME", "PRIVACYACKNOWLEDGEMENT",
                                    "PRIVACYTIMESTAMP", "KNOWNUMBERS", "KNOWNUMBERSINFO", "AGE",
                                    "SEXATBIRTH", "OTHERSTATINS", "OTHERSTATINSINFO", "CYCLOSPORINE",
                                    "CYCLOSPORINEINFO", "WARFARIN", "WARFARININFO", "HEARTATTACK", "STROKE",
                                    "PROCEDURE", "PAD", "CARDIACINFO", "ASCVDFAMILYHISTORYFATHER",
                                    "ASCVDFAMILYHISTORYMOTHER", "TRIGLYCERIDES", "TRIGLYCERIDESINFO",
                                    "TOTALCHOLESTEROL", "LDLCHOLESTEROL",
                                    "HDLCHOLESTEROL", "CHOLESTEROLINFO", "ASCVDRISKSCORE", "LIVERDISEASE",
                                    "LIVERDISEASEINFO", "KIDNEYDISEASE",
                                    "KIDNEYDISEASEINFO", "ALCOHOLUSAGE", "ALCOHOLUSAGEINFO", "CHOLESTEROLMEDSREACTION",
                                    "CHOLESTEROLMEDSREACTIONINFO", "OTHERMEDS"]

# List of all potential adverse email timestamp fields
ADVERSE_EMAIL_FIELDS = ["HeartAttackAEEmailTimestamp", "StrokeAEEmailTimestamp", "ProcedureAEEmailTimestamp",
                        "PADAEEmailTimestamp", "LDLAEEmailTimestamp", "PregnantAEEmailTimestamp",
                        "LiverAEEmailTimestamp",
                        "MuscleAEEmailTimestamp", "KidneyAEEmailTimestamp"]

pathName = ["Initial Assessment_Clinicians Records.csv", "Initial Assessment_Participant Records.csv",
            "Reassessment_Participant Records.csv", "Reassessment_Clinicians Records.csv"]


# Verifies that if LDL retested, the relevant fields are filled
def verify_ldl_percentage_value(metric_map, ia_metric_maps, is_initial):
    if metric_map.get("OUTCOME") in ["OK", "AAD"]:
        ia_metric_maps_filtered = ia_metric_maps.map(lambda it: it.get("LDLCHOLESTEROL") is not None)
        if ia_metric_maps_filtered.empty:
            return "No data to compare"
        ia_metric_map = ia_metric_maps_filtered.last()
        if metric_map.get("LDLCHOLESTEROLRETESTED") == "TRUE":
            old_ldl = ia_metric_map.get("LDLCHOLESTEROL").toDouble()
            new_ldl = metric_map.get("LDLCHOLESTEROL").toDouble()
            diff = ((new_ldl - old_ldl) / old_ldl) * 100
            return passed_if_true(diff == metric_map.get("LDLCHOLESTEROLPERCENTAGECHANGE").toDouble())
        elif metric_map.get("LDLCHOLESTEROLRETESTED") == "FALSE":
            ia_privacy_date = datetime.fromtimestamp(ia_metric_map.get("PRIVACYTIMESTAMP"))
            ra_privacy_date = datetime.fromtimestamp(metric_map.get("PRIVACYTIMESTAMP"))
            td9 = ra_privacy_date - ia_privacy_date
            if not is_initial and td9 > timedelta(months=9):
                return passed_if_true(metric_map.get("OUTCOME") == "AAD")
            else:
                return "N/A - LDL not retested"
        elif metric_map.get("KIDNEYAEEMAILTIMESTAMP") is not "":
            return "N/A - skipped for Kidney Disease"
    return "N/A: Outcome EXIT, TIME, DNU"


# Verify LDL-related fields are entered if LDL is filled in
def verify_ldl_percentage_fields(metric_map):
    if metric_map.get("OUTCOME") in ["OK", "AAD"]:
        if metric_map.get("LDLCHOLESTEROLRETESTED") == "TRUE":
            return fields_should_be_filled_in(
                ["LDLCHOLESTEROL", "LDLCHOLESTEROLPERCENTAGECHANGE", "LDLCHOLESTEROLINFO"], metric_map)
        elif metric_map.get("LDLCHOLESTEROLRETESTED") in ["", "FALSE"] and (metric_map.get("KIDNEYAEEMAILTIMESTAMP") is
                                                                            not ""):
            return "N/A - skipped for Kidney Disease"
        else:
            return "N/A - LDL not retested"
    return "N/A: Outcome EXIT, TIME, DNU"


# Verifies AE timestamp fields are filled for appropriate conditions
def adverse_emails_accurate(metric_map, ae_fields):
    results = []
    if metric_map.get("OUTCOME") == "DNU":
        k = metric_map.get("KICKOUTTYPE")
        relevant_kickouts = ["HEART ATTACK", "STROKE", "PAD", "PROCEDURE", "LDL", "PREGNANT", "LIVER", "MUSCLE"]
        for it in relevant_kickouts:
            if k.contains(it):
                ae_timestamp_field = ae_fields.map(lambda it2: it2.contains(it))
                results.append(passed_if_true(metric_map.get(ae_timestamp_field) is not ""))
        return passed_if_true("FAILED" not in results)
    elif "TRUE" in [metric_map.get("KIDNEYDISEASE"), metric_map.get("KIDNEYDISEASEWORSENING")]:
        return passed_if_true(metric_map.get("KIDNEYAEEMAILTIMESTAMP") is not "")
    else:
        return "N/A: Not DNU or Kidney Disease"


# Verifies Kidney Disease response
def verify_kidney_reassessment(metric_map, ia_metric_maps):
    conditions = []
    if metric_map.get("OUTCOME") in ["AAD", "OK"]:
        ia_metric_maps_filtered = ia_metric_maps.map(lambda it: it.get("KIDNEYDISEASE") is not "")
        if ia_metric_maps_filtered.empty:
            return "No IA data to compare"
        ia_metric_map = ia_metric_maps_filtered.last()
        kw = metric_map.get("KIDNEYDISEASEWORSENING")
        ae = metric_map.get("KIDNEYAEEMAILTIMESTAMP")
        if ia_metric_map.get("KIDNEYDISEASE") == "TRUE":
            conditions.append(kw in ["TRUE", "FALSE"])
            if kw == "FALSE":
                conditions.append(ae == "")
            else:
                conditions.append(ae is not "")
        elif metric_map.get("KIDNEYDISEASE") == "TRUE":
            conditions.append(kw == "")
            conditions.append(ae is not "")
        else:
            return "N/A-No Kidney Disease"
        return passed_if_true(False not in conditions)
    return "N/A: Outcome EXIT, DNU"


# Returns list of additional REFs from the additional REF flow
def get_additional_refs_present(metric_map):
    refs = []
    if metric_map.get("PSORIASISRA") == "TRUE":
        refs.append("psoriasisRA")
    if metric_map.get("HIV") == "TRUE":
        refs.append("hiv")
    if metric_map.get("PREECLAMPSIA") == "TRUE":
        refs.append("preeclampsia")
    if metric_map.get("EARLYMENOPAUSE") == "TRUE":
        refs.append("EarlyMenopause")
    return refs


# Returns a list of REFs that would be presented before the "Additional REFs" flow
def get_risk_enhancing_factors_present(metric_map):
    age = metric_map.get("AGE").toInteger()
    gender = metric_map.get("SEXATBIRTH")
    refs = []
    if (gender == "MALE") and (20 <= age) and (age < 40):
        if metric_map.get("ASCVDFAMILYHISTORYMOTHER") == "TRUE":
            refs.append("familyHistoryMother")
        if metric_map.get("ASCVDFAMILYHISTORYFATHER") == "TRUE":
            refs.append("familyHistoryFather")
        if not (metric_map.get("LDLCHOLESTEROL") in ["", None]):
            if 160 <= metric_map.get("LDLCHOLESTEROL").toInteger() < 190:
                refs.append("LDL")
    else:
        if metric_map.get("ETHNICITYRACE") == "SOUTH ASIAN":
            refs.append("race")
        if 175 <= metric_map.get("TRIGLYCERIDES").toInteger() < 500:
            refs.append("TRIGLYCERIDES")
    if len(get_met_syndrome_factors(metric_map)) > 3:
        refs.append("MetSyn")
    return refs


# Returns a list of MetSyn factors
def get_met_syndrome_factors(metric_map):
    age = metric_map.getOrDefault("AGE", "0").toInteger()
    gender = metric_map.getOrDefault("SEXATBIRTH", "RA")
    met_syn_factors = []
    tg = metric_map.getOrDefault("TRIGLYCERIDES", "0").toInteger()
    hdl = metric_map.getOrDefault("HDLCHOLESTEROL", "0").toInteger()
    if not (gender == "MALE" and age < 40):
        sysbp = metric_map.getOrDefault("SYSTOLICBP", "0").toInteger()
        diasbp = metric_map.getOrDefault("DIASTOLICBP", "0").toInteger()
        if 130 < sysbp < 180:
            met_syn_factors.append("SYSTOLICBP")
        if 80 < diasbp <= 120:
            met_syn_factors.append("DIASTOLICBP")
        if metric_map.get("BPMEDS") == "TRUE" and not (0 in [sysbp, diasbp]) and (sysbp < 130 or diasbp < 80):
            met_syn_factors.append("BP meds")
        if 150 < tg <= 174:
            met_syn_factors.append("TRIGLYCERIDES")
    if gender == "MALE" and 20 < age <= 75:
        if 20 < hdl <= 39:
            met_syn_factors.append("Male HDL between 20 and 39")
    if gender == "FEMALE" and 50 < age <= 75:
        if 20 < hdl <= 49:
            met_syn_factors.append("Female HDL between 20 and 49")
    return met_syn_factors


# If race is South Asian, verifies FH mother and father were not asked and metrics for response are blank
def verify_fh_blank_if_sa(metric_map):
    if metric_map.get("RACE") == "SOUTH ASIAN":
        return passed_if_true((metric_map.get("ASCVDFAMILYHISTORYMOTHER") == "") and
                              (metric_map.get("ASCVDFAMILYHISTORYMOTHER") == ""))
    return "N/A"


# Retrieves the list of required fields appropriate to the condition (IA/RA, Clinician/Participant)
def get_required_fields(is_ia, is_participant):
    if is_participant:
        if is_ia:
            return ALL_IA_PARTICIPANT_REQUIRED_FIELDS
        else:
            return ALL_RA_PARTICIPANT_REQUIRED_FIELDS
    else:
        if is_ia:
            return ALL_IA_CLINICIAN_REQUIRED_FIELDS
        else:
            return ALL_RA_CLINICIAN_REQUIRED_FIELDS


# Verifies pregnancy-related fields are filled if user is female
def pregnancy_filled_in(metric_map, is_ia, ia_metric_records):
    ia_metric_records_2 = ia_metric_records
    kickout = None
    if metric_map.get("OUTCOME") in ["DNU", "OK", "AAD"]:
        kickout = metric_map.get("KICKOUTTYPE")
    if not (kickout in ["AGE", "NUMBERS", "HEART ATTACK", "PAD", "STROKE", "LDL"]) and not is_ia:
        if not is_ia:
            ia_metric_records_2 = ia_metric_records.map(lambda it: it.get("SEXATBIRTH") is not None)
        if ia_metric_records_2.empty:
            return "No IA records"
        if (not ia_metric_records_2.map(lambda it: it.get("SEXATBIRTH") == "FEMALE").empty
                and not ia_metric_records_2.map(lambda it: it.get("SEXATBIRTH") == "MALE").empty):
            ia_metric_record = ia_metric_records_2.last()
            sex_at_birth = ia_metric_record.get("SEXATBIRTH")
        else:
            sex_at_birth = metric_map.get("SEXATBIRTH")
        if sex_at_birth == "FEMALE":
            return fields_should_be_filled_in(["PREGNANT", "PREGNANTINFO"], metric_map)
        elif sex_at_birth == "MALE":
            return fields_should_not_be_filled_in(["PREGNANT", "PREGNANTINFO"], metric_map)
        return "N/A - Prior kickout: " + metric_map.get("KICKOUTTYPE")
    return "N/A: Outcome TIME, EXIT"


# Verifies all required fields are filled in
def verify_no_missing_inputs(metric_map, is_ia, is_participant):
    outcome = metric_map.get("OUTCOME")
    if outcome in ["OK", "AAD"]:
        all_input_fields = get_required_fields(is_ia, is_participant)
        return fields_should_be_filled_in(all_input_fields, metric_map)
    else:
        return "N/A: Outcome EXIT, TIME, DNU"


# Verifies no outcomes are blank
def verify_no_blank_outcomes(metric_map):
    return passed_if_true(metric_map.get("OUTCOME") is not None)


# fields - list of fields. All fields in list are expected NOT to be filled in
# metric_map - mapping of metric name : metric value of current metric
# returns "PASSED" if no fields in the list are filled in, "FAILED" if not
def fields_should_not_be_filled_in(fields, metric_map):
    function_results = [{}]
    for it in fields:
        function_results.append({it + " is NOT filled in": passed_if_true(metric_map.get(it))})
    return passed_if_true("FAILED" not in function_results)


# fields - list of fields. All fields in list are expected to be filled in
# metric_map - mapping of metric name : metric value of current metric
# returns "PASSED" if all fields in the list are filled in, "FAILED" if not
def fields_should_be_filled_in(fields, metric_map):
    function_results = [{}]
    for it in fields:
        function_results.append({it + " is filled in": passed_if_true(metric_map.get(it) is not "")})
    return passed_if_true("FAILED" not in function_results)


# metric_map -  mapping of metric name : metric value of current metric
# kickout_type - value of kickout type as reported in the KICKOUTTYPE metric
# returns - "PASSED" if all cholesterol med-related values in the kickout type also possess "TRUE" for their
# corresponding metric
def verify_chol_meds_dnu_accurate(metric_map, kickout_type):
    final_state = True
    if kickout_type.contains("CHOL/TRIG LOWERING MEDS"):
        final_state = final_state and metric_map.get("OTHERSTATINS") == "TRUE"
    for it in ["CYCLOSPORINE", "WARFARIN"]:
        if kickout_type.contains(it):
            final_state = final_state and metric_map.get(it) == "TRUE"
    return passed_if_true(final_state)


# metric_map -  mapping of metric name : metric value of current metric
# kickout_type - value of kickout type as reported in the KICKOUTTYPE metric
# returns - "PASSED" if all cardiac-related values in the kickout type also possess "TRUE" for their
# corresponding metric
def verify_cardiac_accurate(metric_map, kickout_type):
    for it in ["HEART ATTACK", "STROKE", "PROCEDURE", "PAD"]:
        if it not in kickout_type and metric_map.get(it.replace(" ", "")) == "TRUE":
            return "FAILED"
    return "PASSED"


# metric_map -  mapping of metric name : metric value of current metric
# kickout_type - value of kickout type as reported in the KICKOUTTYPE metric
# returns - "PASSED" if all BP-related values in the kickout type also possess "TRUE" for their
# corresponding metric
def verify_bp_kickout_accurate(metric_map, kickout_type):
    final_state = True
    diastolic = metric_map.get("DIASTOLICBP").toInteger()
    systolic = metric_map.get("SYSTOLICBP").toInteger()
    diff = systolic - diastolic

    if kickout_type.contains("DIASTOLIC BP LOW"):
        final_state = final_state and diastolic < 50
    if kickout_type.contains("DIASTOLIC BP HIGH"):
        final_state = final_state and diastolic > 120
    if kickout_type.contains("SYSTOLIC BP LOW"):
        final_state = final_state and systolic < 90
    if kickout_type.contains("SYSTOLIC BP HIGH"):
        final_state = final_state and systolic > 180
    if kickout_type.contains("PULSE PRESSURE DIFFERENTIAL"):
        final_state = final_state and diff <= 20 or diff > 80
    return passed_if_true(final_state)


# Verify for a kickout type reported, other metrics are in a configuration that would result in said kickout type
# according to the flow chart
def compare_kickout_type_with_metrics(metric_map, is_ia):
    if metric_map.get("OUTCOME") == "DNU":
        k = metric_map.get("KICKOUTTYPE").trim()
        if k in ["PAD", "STROKE", "HEART ATTACK", "PROCEDURE"]:
            return verify_cardiac_accurate(metric_map, k)
        if k in ["CHOL/TRIG LOWERING MEDS", "CYCLOSPORINE", "WARFARIN"]:
            return verify_chol_meds_dnu_accurate(metric_map, k)
        if k in ["DIASTOLIC BP LOW", "DIASTOLIC BP HIGH", "SYSTOLIC BP LOW", "SYSTOLIC BP HIGH",
                 "PULSE PRESSURE DIFFERENTIAL"]:
            return verify_bp_kickout_accurate(metric_map, k)
        if k == "PREGNANT":
            return passed_if_true(metric_map.get("PREGNANT") == "TRUE")
        if k == "KNOW NUMBERS":  # Haven't seen example
            return passed_if_true(metric_map.get("KNOWNUMBERS") == "FALSE")
        if k == "FEMALE AGE":
            if is_ia:
                return passed_if_true(verify_user_age(metric_map) == "FAILED")
            return "N/A"
        if k == "TC LOW":
            return passed_if_true(metric_map.get("TOTALCHOLESTEROL").toInteger() < 130)
        if k == "TC HIGH":
            return passed_if_true(metric_map.get("TOTALCHOLESTEROL").toInteger() > 320)
        if k == "HDL LOW":
            return passed_if_true(metric_map.get("HDLCHOLESTEROL").toInteger() < 20)
        if k == "HDL HIGH":
            return passed_if_true(metric_map.get("HDLCHOLESTEROL").toInteger() > 100)
        if k == "LDL LOW":
            ldl_c = metric_map.get("LDLCHOLESTEROL").toInteger()
            min_age = 40
            if metric_map.get("SEXATBIRTH") == "FEMALE":
                min_age = 50
            if metric_map.get("AGE").toInteger() >= min_age:
                return passed_if_true(ldl_c < 70)
            else:
                return passed_if_true(ldl_c < 160)
        if k == "LDL HIGH":
            ldl_c = metric_map.get("LDLCHOLESTEROL").toInteger()
            return passed_if_true(ldl_c >= 190)
        if k == "LDL CHOLESTEROL LESS THAN 160 MALES 20-39":
            ldl_c = metric_map.get("LDLCHOLESTEROL").toInteger()
            return passed_if_true(metric_map.get("SEXATBIRTH") == "MALE" and metric_map.get("AGE").toInteger() < 40 and
                                  ldl_c < 160)
        if k == "NO FAMILY HISTORY MALES 20-39":
            return passed_if_true(metric_map.get("SEXATBIRTH") == "MALE" and metric_map.get("AGE").toInteger() < 40 and
                                  metric_map.get("ASCVDFAMILYHISTORYMOTHER") == "FALSE" and metric_map.get(
                "ASCVDFAMILYHISTORYFATHER") == "FALSE")
        if k in ["HASLIVERDISEASE", "LIVER SYMPTOMS"]:
            if is_ia:
                return passed_if_true(metric_map.get("LIVERDISEASE") == "TRUE")
            else:
                return passed_if_true(metric_map.get("LIVERSYMPTOMS") == "TRUE")
        if k == "MUSCLE SYMPTOMS":
            return passed_if_true(metric_map.get("MUSCLESYMPTOMS") == "TRUE")
        if k == "DIABETIC AGE":
            min_age = 50
            if metric_map.get("SEXATBIRTH") == "FEMALE":
                min_age = 60
            return passed_if_true(metric_map.get("DIABETES") == "TRUE" and metric_map.get("AGE").toInteger() > min_age)
        if k == "TG HIGH":
            return passed_if_true(metric_map.get("TRIGLYCERIDES").toInteger() >= 500)
        if k == "RISK SCORE LOW":
            return passed_if_true(metric_map.get("ASCVDRISKSCORE").toDouble() < 5)
        if k == "RISK SCORE HIGH":
            return passed_if_true(metric_map.get("ASCVDRISKSCORE").toDouble() >= 20)
        if k == "NO REF":
            r = get_risk_enhancing_factors_present(metric_map)
            return passed_if_true(len(r) == 0)
        if k == "NO LDL CHOLESTEROL RETEST":
            return passed_if_true(metric_map.get("LDLCHOLESTEROLRETESTED") == "FALSE")
        if k == "INADEQUATE DECREASE ON FOLLOW UP LDL CHOLESTEROL RETEST":
            ldlp = metric_map.get("LDLCHOLESTEROLPERCENTAGECHANGE").toDouble()
            return passed_if_true(-15 <= ldlp < 0)
        if k == "INCREASE OR NO DECREASE IN LDL CHOLESTEROL":
            ldlp = metric_map.get("LDLCHOLESTEROLPERCENTAGECHANGE").toDouble()
            return passed_if_true(ldlp >= 0)
    return "N/A"


# Converts boolean True/False value to string "PASSED"/"FAILED" value
def passed_if_true(condition):
    if condition:
        return "PASSED"
    else:
        return "FAILED"


# Verified TALKTOADOCTOR is filled in if outcome is AAD
def verify_aad_fields(metric_map):
    if metric_map.get("OUTCOME") == "AAD":
        confirm_result = verify_aad_confirm_and_timestamp(metric_map)
        factor_present = one_factor_for_aad(metric_map)
        return passed_if_true(not ("FAILED" in [confirm_result, factor_present]))
    return "N/A"


# Returns "PASSED" if CONFIRMDOCTOR and DOCTORTIMESTAMP filled
def verify_aad_confirm_and_timestamp(metric_map):
    if metric_map.get("OUTCOME") == "AAD":
        ttd = metric_map.get("TALKTOADOCTOR")
        confirm = metric_map.get("CONFIRMDOCTOR")
        if ttd == "":
            return fields_should_not_be_filled_in(["CONTINUETOBUY", "CONFIRMDOCTOR", "DOCTORTIMESTAMP"], metric_map)
        else:
            ctb = metric_map.get("CONTINUETOBUY")
            if confirm != "":
                confirm = fields_should_be_filled_in(["DOCTORTIMESTAMP"], metric_map)
            else:
                confirm = fields_should_not_be_filled_in(["DOCTORTIMESTAMP"], metric_map)
        if ttd == "FALSE":
            return passed_if_true(ctb == "" and confirm == "PASSED")
        else:
            return passed_if_true(ctb is not "" and confirm == "PASSED")
    return "N/A"


# returns "PASSED" if in DNU scenario kickout fields are filled in
def verify_dnu_fields(metric_map):
    if metric_map.get("OUTCOME") == "DNU":
        return fields_should_be_filled_in(["KICKOUTTYPE", "KICKOUTTIMESTAMP"], metric_map)
    return "N/A"


# returns "PASSED" if confirmation fields are filled where appropriate
def verify_confirm_fields(metric_map):
    if metric_map.get("OUTCOME") in ["AAD", "OK"]:
        return fields_should_be_filled_in(["CONFIRMANSWERS", "CONFIRMATIONCHECKBOX", "CHECKBOXTIMESTAMP"], metric_map)
    return "N/A"


# returns "PASSED" if confirmation fields are filled where appropriate
def verify_ok_accurate(metric_map):
    if metric_map.get("OUTCOME") == "OK":
        return verify_confirm_fields(metric_map)
    return "N/A"


# returns "PASSED" if this is an AAD scenario and one of the four below responses is "TRUE".
# In any AAD scenario at least one would have to be "TRUE"
def one_factor_for_aad(metric_map):
    responses = []
    if metric_map.get("OUTCOME") == "AAD":
        responses.append(metric_map.get("KIDNEYDISEASE"))
        responses.append(metric_map.get("KIDNEYDISEASEWORSENING"))
        responses.append(metric_map.get("ALCOHOLUSAGE"))
        responses.append(metric_map.get("CHOLESTEROLMEDSREACTION"))
        responses.append(metric_map.get("OTHERMEDS"))
        return passed_if_true("TRUE" in responses)
    return "N/A"


# - "PASSED" if user is within an age range that would allow them to proceed with the assessment
def verify_user_age(metric_map):
    age = metric_map.getOrDefault("AGE", "N/A")
    min_age = 50
    max_age = 75
    if age is not "N/A":
        gender = metric_map.get("SEXATBIRTH")
        if gender == "MALE":
            min_age = 20
        return passed_if_true(min_age <= age < max_age)
    return "FAILED"


# Verifies follow-up metrics are filled when CRP and CAC have test values
def verify_cac_crp_tested_fields(metric_map):
    results = []
    if len(get_additional_refs_present(metric_map)) == 0:
        if metric_map.get("HSCRPTESTED") == "TRUE":
            results.append(metric_map.get("HSCRPGT2") is not "")
        elif metric_map.get("CACTESTED") == "TRUE":
            if metric_map.get("AGE").toInteger() >= 55:
                results.append(metric_map.get("CACGT0") is not "")
            else:
                results.append(metric_map.get("CAC100ORG") is not "")
        return passed_if_true("FAILED" not in results)
    return "N/A"


# Verifies the correct path is taken in terms or gathering additional data and potential additional REFs
def verify_refs_accurate(metric_map):
    results = []
    total_refs = []
    if metric_map.get("OUTCOME") in ["AAD", "OK"]:
        age = metric_map.get("AGE").toInteger()
        gender = metric_map.get("SEXATBIRTH")
        waist_size_field = "WAISTSIZE35"
        if gender == "MALE":
            waist_size_field = "WAISTSIZE40"
            total_refs = get_risk_enhancing_factors_present(metric_map)
        ms_flags = get_met_syndrome_factors(metric_map)
        if len(total_refs) == 0 and (not (gender == "MALE" and age < 40)) and metric_map.get("DIABETES") == "FALSE":
            if len(ms_flags) == 2:
                results.append(fields_should_not_be_filled_in([waist_size_field, "WAISTSIZEINFO"], metric_map))
            if metric_map.get(waist_size_field) == "TRUE":
                ms_flags.append("Waist Size")
                results.append(fields_should_be_filled_in(["PSORIASISRA", "HIV"], metric_map))
                if gender == "FEMALE":
                    results.append(fields_should_be_filled_in(["PREECLAMPSIA", "EARLYMENOPAUSE"], metric_map))
                results.append(verify_cac_crp_tested_fields(metric_map))
                if metric_map.get("CACGT0") == "TRUE" or metric_map.get("CAC100ORG") == "TRUE":
                    total_refs.append("CAC")
        else:
            results.append(fields_should_not_be_filled_in([waist_size_field, "WAISTSIZEINFO", "PSORIASISRA", "HIV",
                                                           "PREECLAMPSIA", "HSCRPTESTED", "CACTESTED", "HSCRPGT2",
                                                           "CAC100ORG", "CACGT0"], metric_map))
        return passed_if_true(results not in [False, "FAILED"])
    return "N/A: Outcome EXIT, DNU"


# lines - a list of strings representing the lines of the csv file
# returns a list of map entries, which represent "metric name":"metric value"
def read_unfiltered_map(this_csv):
    local_record_map = [{}]
    record_list = [[{}]]
    df = pd.read_csv(this_csv)
    metrics_columns = df.first(0)
    for index, row in df.iterrows():
        if index != 0:
            for j, entry in metrics_columns:
                if j >= metrics_columns.size():
                    local_record_map.append({entry.toUpperCase(): ""})
                else:
                    local_record_map.append({entry.toUpperCase(): metrics_columns.get(j).toUpperCase()})
            record_list.append(local_record_map)
    return record_list


# Process each CSV file by iterating through the file names, creating a corresponding sheet, reading in data,
# making the verifications, and outputting the results within rows on the sheet
def __main__():
    for it in pathName:
        # Different verifications may have different expected results depending on whether data is IA/RA,
        # Participant/Clinician
        current_ia = "Initial" in it
        current_participant = "Participant" in it
        record_list = read_unfiltered_map(it)

        # Loop through records (lines in CSV)
        for j, record_map in record_list:
            results = [{}]
            results.append({"Metric ID": record_map.get("METRICID")})
            results.append({"Privacy Timestamp": record_map.get("PRIVACYTIMESTAMP")})
            results.append({"Participant ID": record_map.get("PARTICIPANTID")})
            results.append({"Outcome": record_map.get("OUTCOME")})
            if record_map.getOrDefault("OUTCOME", "") is not "":
                results.append(
                    {"No Missing Inputs": verify_no_missing_inputs(record_map, current_ia, current_participant)})
                results.append({"Outcome Is Not Blank": verify_no_blank_outcomes(record_map)})
            if not current_ia:
                if current_participant:
                    ia_metric_records = record_map.map(
                        lambda par: record_map.get("PARTICIPANTID") == par.get("PARTICIPANTID")).unique()
                else:
                    ia_metric_records = record_map.map(
                        lambda clinician: record_map.get("PARTICIPANTID") == clinician.get("PARTICIPANTID"))
                results.append({"Pregnancy Fields Accurate": pregnancy_filled_in(record_map, current_ia,
                                                                                 current_participant)})
                results.append({"LDL % Fields Present": verify_ldl_percentage_fields(record_map)})
                results.append({"LDL % Accurate": verify_ldl_percentage_value(record_map, ia_metric_records, True)})
                results.append({"Kidney Worsening Accurate": verify_kidney_reassessment(record_map, ia_metric_records)})
                results.append({"Adverse Emails Accurate": adverse_emails_accurate(record_map, ADVERSE_EMAIL_FIELDS)})
            else:
                results.append(
                    {"Pregnancy Fields Accurate": pregnancy_filled_in(record_map, current_ia, current_participant)})
                results.append({"REF Accurate": verify_refs_accurate(record_map)})
            results.append({"Outcome not overwritten with TIME/EXIT": passed_if_true(
                record_map.get("CONFIRMATION") is not "TRUE")})
            results.append({"OK Outcome Accurate": verify_ok_accurate(record_map)})
            results.append({"DNU Accurate": verify_dnu_fields(record_map)})
            results.append({"Kickout Description": record_map.get("KICKOUTTYPE")})
            results.append({"Kickout Description Accurate": compare_kickout_type_with_metrics(record_map, current_ia)})
            results.append({"AAD Conditions Met Accurate": one_factor_for_aad(record_map)})
            results.append({"AAD Outcome Accurate": verify_aad_fields(record_map)})
            results.append({"AAD Confirmation To Purchase": verify_aad_confirm_and_timestamp(record_map)})

            file_path = "output" + str(datetime.now()) + ".xls"
            sheet_name = "Sheet1"
            df = pd.DataFrame(r, columns=IA_ALL_COLUMNS)
            df.to_excel(file_path, sheet_name)

            with pd.ExcelWriter(file_path, mode='a') as writer:
                for r in results:
                    df = pd.DataFrame(r)
                    df.to_excel(writer, sheet_name)
