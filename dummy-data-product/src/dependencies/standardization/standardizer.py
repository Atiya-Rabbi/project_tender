import logging
logging.basicConfig(level=logging.INFO)
import pandas as pd
import os,requests,time
from dependencies.utils.utils import remove_prevfiles, load_csv
from configurations.helper import read_config


class Standardizer:
    def __init__(self) -> None:
        config = read_config()
        self.geocoder_data = config['CSVFiles']['geocoder']
        self.stnd_data = config['CSVFiles']['standard']
        self.url = config['URLs']['scrape_url']

    def identified_status(self,status):
        
        if status == "AWARDED":
            return "closed"
        elif status == "PREAWARDED" or status == "PREPARED":
            return "Pipeline"
        elif status == "ANNOUNCED":
            return "Active"
        elif status == "CANCELLED":
            return "Canceled"
        else:
            return "Not Available"
    
    def all_timestamp(self,df):
        timestamps = {
            'tender_awardDeadline':      df['tender_awardDeadline'],
            'tender_awardDecisionDate':  df['tender_awardDecisionDate'],
            'tender_bidDeadline':        df['tender_bidDeadline'],
            'tender_cancellationDate':   df['tender_cancellationDate'],
            'tender_estimatedStartDate': df['tender_estimatedStartDate'],
            'tender_estimatedCompletionDate':              df['tender_estimatedCompletionDate'],
            'tender_publications_firstCallForTenderDate':  df['tender_publications_firstCallForTenderDate'],
            'tender_publications_lastCallForTenderDate':   df['tender_publications_lastCallForTenderDate'],
            'tender_publications_firstdContractAwardDate': df['tender_publications_firstdContractAwardDate'],
            'tender_publications_lastContractAwardDate':   df['tender_publications_lastContractAwardDate'],
            'tender_contractSignatureDate':                df['tender_contractSignatureDate'],
            
        }
        return timestamps
    
    def standardize_data(self):
        try:
            remove_prevfiles(file_name=self.stnd_data)
            logging.info('Removed old Standardized data file')
            #read csv
            geo_data = load_csv(self.geocoder_data)
            logging.info('Geocoded data--> {}'.format(self.geocoder_data))
            geo_data['project_or_tender'] = 'T'
            geo_data['source'] = 'opentender IRELAND'
            geo_data['url'] = self.url
            geo_data["identified_status"] = geo_data.apply(lambda x: self.identified_status(x['lot_status']),axis=1)
            replace_cols = {
                'tender_row_nr':'aug_id', 'tender_id':'original_id', 'tender_supplyType':'identified_sector_subsector_tuple','tender_size':'keywords',
                'tender_title':'name','buyer_mainActivities':'description',
                'lot_status':'status', 'tender_finalPrice_currency':'currency','tender_finalPrice':'budget', 'tender_publications_lastContractAwardUrl': 'document_urls',
                'tender_supplyType': 'sector', 'buyer_mainActivities': 'subsector', 'buyer_buyerType': 'identified_sector',
                'buyer_country': 'country_code', 'buyer_name':'entities'
            }
            geo_data.rename(columns = replace_cols, inplace = True)
            geo_data['timestamps'] = geo_data.apply(lambda x: self.all_timestamp(x), axis=1)
            drop_cols = {
                'tender_mainCpv', 'tender_cpvs', 'tender_addressOfImplementation_nuts', 'tender_year', 'tender_country',
                'tender_eligibleBidLanguages', 'tender_npwp_reasons', 'tender_awardDeadline', 'tender_contractSignatureDate',
                'tender_awardDecisionDate', 'tender_bidDeadline', 'tender_cancellationDate', 'tender_estimatedStartDate',
                'tender_estimatedCompletionDate', 'tender_estimatedDurationInYears', 'tender_estimatedDurationInMonths',
                'tender_estimatedDurationInDays','tender_isEUFunded', 'tender_isDps', 'tender_isElectronicAuction', 'tender_isCentralProcurement',
                'tender_isJointProcurement', 'tender_isOnBehalfOf', 'tender_isFrameworkAgreement', 'tender_isCoveredByGpa','tender_hasLots',
                'tender_estimatedPrice', 'tender_estimatedPrice_currency', 'tender_estimatedPrice_minNetAmount', 'tender_estimatedPrice_maxNetAmount',
                'tender_estimatedPrice_EUR', 'tender_finalPrice_minNetAmount', 'tender_finalPrice_maxNetAmount', 'tender_finalPrice_EUR',
                'tender_description_length', 'tender_personalRequirements_length', 'tender_economicRequirements_length', 'tender_technicalRequirements_length',
                'tender_documents_count', 'tender_awardCriteria_count', 'tender_corrections_count', 'tender_onBehalfOf_count', 
                'tender_lots_count', 'tender_publications_count', 'tender_indicator_INTEGRITY_SINGLE_BID', 'tender_indicator_INTEGRITY_CALL_FOR_TENDER_PUBLICATION',
                'tender_indicator_INTEGRITY_ADVERTISEMENT_PERIOD', 'tender_indicator_INTEGRITY_PROCEDURE_TYPE', 'tender_indicator_INTEGRITY_DECISION_PERIOD',
                'tender_indicator_INTEGRITY_TAX_HAVEN', 'tender_indicator_INTEGRITY_NEW_COMPANY','tender_indicator_ADMINISTRATIVE_CENTRALIZED_PROCUREMENT',
                'tender_indicator_ADMINISTRATIVE_ELECTRONIC_AUCTION', 'tender_indicator_ADMINISTRATIVE_COVERED_BY_GPA', 'tender_indicator_ADMINISTRATIVE_FRAMEWORK_AGREEMENT',
                'tender_indicator_ADMINISTRATIVE_ENGLISH_AS_FOREIGN_LANGUAGE','tender_indicator_ADMINISTRATIVE_NOTICE_AND_AWARD_DISCREPANCIES',
                'tender_indicator_TRANSPARENCY_NUMBER_OF_KEY_MISSING_FIELDS', 'tender_indicator_TRANSPARENCY_AWARD_DATE_MISSING',
                'tender_indicator_TRANSPARENCY_BUYER_NAME_MISSING', 'tender_indicator_TRANSPARENCY_PROC_METHOD_MISSING',
                'tender_indicator_TRANSPARENCY_BUYER_LOC_MISSING', 'tender_indicator_TRANSPARENCY_BIDDER_ID_MISSING', 'tender_indicator_TRANSPARENCY_BIDDER_NAME_MISSING',
                'tender_indicator_TRANSPARENCY_MARKET_MISSING', 'tender_indicator_TRANSPARENCY_TITLE_MISSING', 'tender_indicator_TRANSPARENCY_VALUE_MISSING',
                'tender_indicator_TRANSPARENCY_YEAR_MISSING', 'tender_indicator_INTEGRITY_WINNER_CA_SHARE', 'tender_indicator_TRANSPARENCY_MISSING_ADDRESS_OF_IMPLEMENTATION_NUTS',
                'tender_indicator_TRANSPARENCY_MISSING_ELIGIBLE_BID_LANGUAGES', 'tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_AWARD_CRITERIA',
                'tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_CPVS', 'tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_DURATION_INFO',
                'tender_indicator_TRANSPARENCY_MISSING_OR_INCOMPLETE_FUNDINGS_INFO', 'tender_indicator_TRANSPARENCY_MISSING_SELECTION_METHOD',
                'tender_indicator_TRANSPARENCY_MISSING_SUBCONTRACTED_INFO', 'buyer_row_nr', 'buyer_id', 'buyer_nuts',
                'buyer_city', 'buyer_postcode', 'lot_row_nr', 'lot_selectionMethod','lot_contractSignatureDate', 'lot_cancellationDate',
                'lot_isAwarded', 'lot_estimatedPrice', 'lot_estimatedPrice_currency', 'lot_estimatedPrice_minNetAmount', 'lot_estimatedPrice_maxNetAmount',
                'lot_estimatedPrice_EUR', 'lot_lotNumber', 'lot_bidsCount', 'lot_validBidsCount', 'lot_smeBidsCount', 'lot_electronicBidsCount',
                'lot_nonEuMemberStatesCompaniesBidsCount', 'lot_otherEuMemberStatesCompaniesBidsCount', 'lot_foreignCompaniesBidsCount',
                'lot_description_length', 'bid_row_nr', 'bid_isWinning', 'bid_isSubcontracted', 'bid_isConsortium', 'bid_price', 'bid_price_currency',
                'bid_price_minNetAmount', 'bid_price_maxNetAmount','bid_price_EUR','bidder_row_nr', 'bidder_id', 'bidder_name',
                'bidder_nuts', 'bidder_city', 'bidder_country', 'bidder_postcode', 'lot_title', 'tender_procedureType', 'tender_nationalProcedureType',
                'tender_awardDeadline','tender_contractSignatureDate','tender_awardDecisionDate','tender_bidDeadline','tender_cancellationDate',
                'tender_estimatedStartDate','tender_estimatedCompletionDate','tender_publications_firstCallForTenderDate', 'tender_publications_lastCallForTenderDate',
                'tender_publications_firstdContractAwardDate', 'tender_publications_lastContractAwardDate','tender_isAwarded'
            }
            geo_data.drop(columns =drop_cols, inplace=True, axis=1)
            new_cols = [
                "aug_id","original_id","project_or_tender","name", "description", "source", "status", "identified_status",
                "currency","budget", "url", "document_urls", "sector", "subsector", "identified_sector", "identified_subsector",
                "identified_sector_subsector_tuple", "keywords", "entities", "country_name", "country_code", "location",
                "map_coordinates", "timestamps"
            ]
            geo_data=geo_data.reindex(columns=new_cols)
            geo_data.to_csv(self.stnd_data, index=False)
            logging.info('Standardized data--> {}'.format(self.stnd_data))
        
        except Exception as e:
            logging.exception(e)
