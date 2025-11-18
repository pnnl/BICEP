"""
Run BICEP analysis for each state individually and combine results.
Following Tim's exact approach and variable naming.
"""

import pandas as pd
from pathlib import Path
from loguru import logger
import sys
from datetime import datetime

# Configure logger
logger.remove()
logger.add(sys.stderr, level="INFO")

from bicep.analysis import BicepResults

# list of all unique states (Tim's variable name)
all_states = [
    'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FL',
    'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME',
    'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH',
    'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI',
    'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY'
]

if __name__ == '__main__':
    logger.info("=== BICEP Individual State Analysis (Tim's Method) ===")
    logger.info("Running each state separately to avoid national competition effects")
    
    # object to store results (Tim's variable names)
    final_results_high = {}
    final_results_bau = {}

    for state in all_states:
        logger.info(f"Processing {state}...")
        
        try:
            # calculate state results (Tim's approach)
            state_results_high = BicepResults(scenario='high', mode='local', target_states=[state])
            state_results_bau = BicepResults(scenario='bau', mode='local', target_states=[state])

            # store results in dictionary (Tim's approach)
            final_results_high[state] = state_results_high
            final_results_bau[state] = state_results_bau
            
            logger.info(f"Completed {state}: BAU ${state_results_bau.total_cost:,.0f}, HIGH ${state_results_high.total_cost:,.0f}")
            
        except Exception as e:
            logger.error(f"Failed to process {state}: {e}")
            continue

    logger.info("All individual state analyses complete. Combining results...")

    # after all states have completed, combine results (Tim's approach)
    # list for aggregate cost data results
    high_aggregate_data = []
    bau_aggregate_data = []
    # list for buildings df data
    high_buildings_list = []
    bau_buildings_list = []
    # list for meta df data
    high_meta_list = []
    bau_meta_list = []

    for state in all_states:
        if state in final_results_high and state in final_results_bau:
            # combined high aggregate data (Tim's approach)
            high_aggregate_data.append({
                'state': state,
                'total_cost': final_results_high[state].total_cost,
                'total_residential_costs': final_results_high[state].total_residential_costs,
                'total_commercial_costs': final_results_high[state].total_commercial_costs
            })

            # combined bau aggregate data (Tim's approach)
            bau_aggregate_data.append({
                'state': state,
                'total_cost': final_results_bau[state].total_cost,
                'total_residential_costs': final_results_bau[state].total_residential_costs,
                'total_commercial_costs': final_results_bau[state].total_commercial_costs
            })

            # collect DataFrames for concatenation (Tim's approach)
            high_buildings_list.append(final_results_high[state].buildings)
            bau_buildings_list.append(final_results_bau[state].buildings)
            high_meta_list.append(final_results_high[state].building_meta)
            bau_meta_list.append(final_results_bau[state].building_meta)

    # Create final DataFrames (Tim's variable names and approach)
    all_states_results_high = pd.DataFrame(high_aggregate_data)
    all_states_results_bau = pd.DataFrame(bau_aggregate_data)

    all_states_buildings_high = pd.concat(high_buildings_list, ignore_index=True)
    all_states_buildings_bau = pd.concat(bau_buildings_list, ignore_index=True)

    all_states_meta_high = pd.concat(high_meta_list, ignore_index=True)
    all_states_meta_bau = pd.concat(bau_meta_list, ignore_index=True)
    
    # Save results with simple naming (no timestamps)
    
    # Save aggregate cost summary results
    all_states_results_high.to_csv("data/parsed_inputs/bicep_cost_summary_high.csv", index=False)
    all_states_results_bau.to_csv("data/parsed_inputs/bicep_cost_summary_bau.csv", index=False)
    
    # Save detailed buildings results  
    all_states_buildings_high.to_csv("data/parsed_inputs/bicep_results_high_all_states.csv", index=False)
    all_states_buildings_bau.to_csv("data/parsed_inputs/bicep_results_bau_all_states.csv", index=False)
    
    # Save meta data
    all_states_meta_high.to_csv("data/parsed_inputs/bicep_meta_high_all_states.csv", index=False)
    all_states_meta_bau.to_csv("data/parsed_inputs/bicep_meta_bau_all_states.csv", index=False)
    
    # Display final totals
    total_bau_cost = all_states_results_bau['total_cost'].sum()
    total_high_cost = all_states_results_high['total_cost'].sum()
    
    logger.info(f"=== FINAL NATIONAL TOTALS (Individual State Method) ===")
    logger.info(f"BAU Total Cost: ${total_bau_cost:,.0f}")
    logger.info(f"HIGH Total Cost: ${total_high_cost:,.0f}")
    logger.info(f"BAU Residential: ${all_states_results_bau['total_residential_costs'].sum():,.0f}")
    logger.info(f"HIGH Residential: ${all_states_results_high['total_residential_costs'].sum():,.0f}")
    logger.info(f"BAU Commercial: ${all_states_results_bau['total_commercial_costs'].sum():,.0f}")
    logger.info(f"HIGH Commercial: ${all_states_results_high['total_commercial_costs'].sum():,.0f}")
    
    logger.info("=== Individual State Analysis Complete ===")