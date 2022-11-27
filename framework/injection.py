# from megalodonclient import Client
import threading
from concurrent.futures import ThreadPoolExecutor

from framework import ResourceInjector
from framework.database import DatabasePool

database_pool = DatabasePool()

database_injector = ResourceInjector(creator=database_pool, destroyer=database_pool.close)



scheduled_type = ['sync_index_price_from_jy', 'sync_index_price_from_jy',
                  'calculate_jy_fund_return_original_weekly_all', 'calculate_jy_fund_return_monthly_all',
                  'calculate_zs_fund_nvs_returns_all', 'calculate_fund_stock_portfolio_exposures_and_residuals',
                  'increment_fund_rank_to_database', 'get_holding_style_index', 'feed_fund_rank_tertile',
                  'month_return_rank_to_database', 'year_return_rank_to_database', 'get_style_exposure_jyfund_cronjob'
                  'feed_fund_bond_risk',
                  'calculate_index_industry_return_and_weight', 'get_fund_index_model_new']
