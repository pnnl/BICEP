"""
Clean, Parse, and Aggregate Technology Stock Data from Nested JSON Files
with Unit Conversion and EDA Integration

This script processes nested JSON files containing technology adoption forecasts,
applies unit conversions, and aggregates the data according to specified mappings.
"""

import json
import pandas as pd
import re
import warnings
from typing import Dict, List, Tuple, Optional, Any
from pathlib import Path


class TechnologyStockParser:
    """
    Main class to parse and aggregate technology stock data from nested JSON files.
    """
    
    def __init__(self, high_json_path: str, stated_json_path: str, tech_map_path: str):
        """
        Initialize parser with file paths.
        
        Args:
            high_json_path: Path to high scenario JSON file
            stated_json_path: Path to stated scenario JSON file  
            tech_map_path: Path to technology mapping CSV file
        """
        self.high_json_path = high_json_path
        self.stated_json_path = stated_json_path
        self.tech_map_path = tech_map_path
        
        # Load data
        self.high_scenario = self._load_json(high_json_path)
        self.stated_scenario = self._load_json(stated_json_path)
        self.tech_mapping = self._load_tech_mapping(tech_map_path)
        
        # Unit conversion registry
        self.unit_registry = self._initialize_unit_registry()
        
        # Track unknown units for warnings
        self.unknown_units = set()
        
    def _load_json(self, file_path: str) -> Dict:
        """Load JSON data from file."""
        with open(file_path, 'r') as f:
            return json.load(f)
    
    def _load_tech_mapping(self, file_path: str) -> pd.DataFrame:
        """Load and validate technology mapping CSV."""
        df = pd.read_csv(file_path)
        
        # Validate required columns
        required_cols = ['Technology', 'WHICH_SCENARIO_HELPER', 'BICEP Technology', 
                        'BICEP Sector', 'BICEP End-Use', 'BICEP Fuel']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing required columns in tech mapping: {missing_cols}")
        
        return df
    
    def _initialize_unit_registry(self) -> Dict[str, float]:
        """
        Initialize unit conversion factors to convert to 'units equipment'.
        All factors convert FROM the key unit TO 'units equipment'.
        """
        return {
            'units equipment': 1.0,  # Base unit
            'TBtu': 1.0,  # Placeholder - needs domain knowledge for conversion
            'MMBtu': 1.0,  # Placeholder - needs domain knowledge for conversion
            'TBtu heating served': 1.0,  # Placeholder
            'TBtu cooling served': 1.0,  # Placeholder
            'units building': 1.0,  # Placeholder - might be 1:1 with equipment
        }
    
    def _extract_unit_from_key(self, key: str) -> Optional[str]:
        """
        Extract unit from measure stock key.
        
        Args:
            key: String like 'Measure Stock (units equipment)' or 'Baseline Stock (TBtu heating served)'
            
        Returns:
            Extracted unit string or None if not found
        """
        match = re.search(r'\((.*?)\)', key)
        if match:
            return match.group(1).strip()
        return None
    
    def _convert_units(self, value: float, from_unit: str, target_unit: str = 'units equipment') -> float:
        """
        Convert value from one unit to another.
        
        Args:
            value: Numeric value to convert
            from_unit: Source unit
            target_unit: Target unit (default: 'units equipment')
            
        Returns:
            Converted value
        """
        if from_unit == target_unit:
            return value
        
        if from_unit not in self.unit_registry:
            self.unknown_units.add(from_unit)
            warnings.warn(f"Unknown unit '{from_unit}' - using value as-is")
            return value
        
        # Convert: value * (conversion_factor_to_base / conversion_factor_from_base)
        # Since all our factors are to base unit, this simplifies to value * factor
        conversion_factor = self.unit_registry[from_unit]
        return value * conversion_factor
    
    def _determine_scenario_source(self, tech_name: str, scenario_helper: str) -> Tuple[str, Dict]:
        """
        Determine which scenario data to use for a given technology.
        
        Args:
            tech_name: Technology name
            scenario_helper: Value from WHICH_SCENARIO_HELPER column
            
        Returns:
            Tuple of (scenario_name, scenario_data)
        """
        if scenario_helper == 'Only in BAU Scenario':
            if tech_name not in self.stated_scenario:
                raise KeyError(f"Technology '{tech_name}' not found in stated scenario")
            return 'bau', self.stated_scenario[tech_name]
        
        elif scenario_helper == 'Only in High Scenario':
            if tech_name not in self.high_scenario:
                raise KeyError(f"Technology '{tech_name}' not found in high scenario")
            return 'high', self.high_scenario[tech_name]
        
        elif scenario_helper == 'Shared':
            # Use high scenario for shared technologies to avoid double counting
            if tech_name not in self.high_scenario:
                if tech_name not in self.stated_scenario:
                    raise KeyError(f"Technology '{tech_name}' not found in either scenario")
                return 'bau', self.stated_scenario[tech_name]
            return 'high', self.high_scenario[tech_name]
        
        else:
            raise ValueError(f"Unknown scenario helper: {scenario_helper}")
    
    def _get_building_types_for_sector(self, sector: str) -> List[str]:
        """
        Get appropriate building types based on sector.
        
        Args:
            sector: Either 'resid' or 'comm'
            
        Returns:
            List of building type strings
        """
        if sector == 'resid':
            return ['Residential (New)', 'Residential (Existing)']
        elif sector == 'comm':
            return ['Commercial (New)', 'Commercial (Existing)']
        else:
            raise ValueError(f"Unknown sector: {sector}")
    
    def _safe_navigate_nested_dict(self, data: Dict, path: List[str]) -> Optional[Any]:
        """
        Safely navigate nested dictionary structure.
        
        Args:
            data: Dictionary to navigate
            path: List of keys to traverse
            
        Returns:
            Value at the end of path or None if path doesn't exist
        """
        current = data
        for key in path:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return None
        return current
    
    def _extract_technology_stock_data(self, tech_name: str, tech_data: Dict, 
                                     scenario_name: str, bicep_mapping: pd.Series) -> List[Dict]:
        """
        Extract stock data for a single technology.
        
        Args:
            tech_name: Name of the technology
            tech_data: Technology data dictionary
            scenario_name: Scenario name ('bau' or 'high')
            bicep_mapping: Series with BICEP mapping information
            
        Returns:
            List of dictionaries with extracted data
        """
        results = []
        
        # Navigate to stock data
        markets_data = self._safe_navigate_nested_dict(
            tech_data, ['Markets and Savings (by Category)', 'Max adoption potential']
        )
        
        if not markets_data:
            warnings.warn(f"No market data found for technology: {tech_name}")
            return results
        
        # Find all measure stock keys
        measure_stock_keys = [key for key in markets_data.keys() 
                             if key.startswith('Measure Stock')]
        
        if not measure_stock_keys:
            warnings.warn(f"No measure stock data found for technology: {tech_name}")
            return results
        
        # Process each measure stock key (different units possible)
        for stock_key in measure_stock_keys:
            stock_data = markets_data[stock_key]
            unit = self._extract_unit_from_key(stock_key)
            
            if not isinstance(stock_data, dict):
                continue
            
            # Get appropriate building types for this sector
            sector = bicep_mapping['BICEP Sector']
            building_types = self._get_building_types_for_sector(sector)
            
            # Iterate through states
            for state in stock_data.keys():
                if not isinstance(stock_data[state], dict):
                    continue
                
                # Iterate through building types
                for building_type in building_types:
                    if building_type not in stock_data[state]:
                        continue
                    
                    building_data = stock_data[state][building_type]
                    if not isinstance(building_data, dict):
                        continue
                    
                    # Iterate through end uses
                    for end_use in building_data.keys():
                        end_use_data = building_data[end_use]
                        if not isinstance(end_use_data, dict):
                            continue
                        
                        # Iterate through fuel types
                        for fuel_type in end_use_data.keys():
                            fuel_data = end_use_data[fuel_type]
                            if not isinstance(fuel_data, dict):
                                continue
                            
                            # Extract yearly data (2024-2050)
                            for year_str, stock_value in fuel_data.items():
                                try:
                                    year = int(year_str)
                                    if 2024 <= year <= 2050:
                                        # Convert units
                                        converted_stock = self._convert_units(
                                            float(stock_value), unit or 'units equipment'
                                        )
                                        
                                        # Map fuel type to BICEP fuel
                                        bicep_fuel = self._map_fuel_type(fuel_type)
                                        
                                        # Map end use to BICEP end use
                                        bicep_end_use = self._map_end_use(end_use)
                                        
                                        results.append({
                                            'year': year,
                                            'stock': converted_stock,
                                            'sector': sector,
                                            'fuel': bicep_fuel,
                                            'end_use': bicep_end_use,
                                            'technology': bicep_mapping['BICEP Technology'],
                                            'state': self._map_state_code(state),
                                            'metric': 'stock',
                                            'scenario': scenario_name,
                                            'building_type': building_type,
                                            'original_unit': unit,
                                            'original_technology': tech_name
                                        })
                                except (ValueError, TypeError) as e:
                                    warnings.warn(f"Error processing {tech_name} data: {e}")
                                    continue
        
        return results
    
    def _map_fuel_type(self, fuel_type: str) -> str:
        """Map original fuel type to BICEP fuel type."""
        fuel_mapping = {
            'Electric': 'electricity',
            'Natural Gas': 'gas',
            'Propane': 'refined liquids',
            'Distillate/Other': 'refined liquids',
            'Biomass': 'biomass'
        }
        return fuel_mapping.get(fuel_type, fuel_type.lower())
    
    def _map_end_use(self, end_use: str) -> str:
        """Map original end use to BICEP end use."""
        end_use_mapping = {
            'Heating (Equip.)': 'heating',
            'Cooling (Equip.)': 'cooling',
            'Water Heating': 'hot water',
            'Ventilation': 'ventilation',
            'Lighting': 'lighting',
            'Refrigeration': 'refrigeration',
            'Cooking': 'cooking',
            'Computers and Electronics': 'electronics',
            'Other': 'other'
        }
        return end_use_mapping.get(end_use, end_use.lower())
    
    def _map_state_code(self, state_code: str) -> str:
        """Map state code to full state name."""
        state_mapping = {
            'AL': 'Alabama', 'AZ': 'Arizona', 'AR': 'Arkansas', 'CA': 'California',
            'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware', 'DC': 'District of Columbia',
            'FL': 'Florida', 'GA': 'Georgia', 'ID': 'Idaho', 'IL': 'Illinois',
            'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas', 'KY': 'Kentucky',
            'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland', 'MA': 'Massachusetts',
            'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi', 'MO': 'Missouri',
            'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada', 'NH': 'New Hampshire',
            'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York', 'NC': 'North Carolina',
            'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma', 'OR': 'Oregon',
            'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }
        return state_mapping.get(state_code, state_code)
    
    def _aggregate_data(self, raw_data: List[Dict]) -> pd.DataFrame:
        """
        Aggregate raw data by grouping similar entries.
        
        Args:
            raw_data: List of dictionaries with raw stock data
            
        Returns:
            Aggregated DataFrame with columns in target order
        """
        if not raw_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_data)
        
        # Group by the required dimensions and sum stock values
        group_cols = ['year', 'sector', 'fuel', 'end_use', 'technology', 'state', 'metric', 'scenario']
        
        # Sum stocks across building types (New + Existing)
        aggregated = df.groupby(group_cols)['stock'].sum().reset_index()
        
        # Reorder columns to match target format: year,stock,sector,fuel,end_use,technology,state,metric,scenario
        target_column_order = ['year', 'stock', 'sector', 'fuel', 'end_use', 'technology', 'state', 'metric', 'scenario']
        aggregated = aggregated[target_column_order]
        
        return aggregated
    
    def parse_all_technologies(self) -> pd.DataFrame:
        """
        Parse all technologies in the mapping file and aggregate results.
        
        Returns:
            DataFrame with aggregated technology stock data
        """
        all_results = []
        
        for idx, row in self.tech_mapping.iterrows():
            tech_name = row['Technology']
            scenario_helper = row['WHICH_SCENARIO_HELPER']
            
            try:
                # Determine which scenario to use
                scenario_name, tech_data = self._determine_scenario_source(tech_name, scenario_helper)
                
                # Extract stock data for this technology
                tech_results = self._extract_technology_stock_data(
                    tech_name, tech_data, scenario_name, row
                )
                
                all_results.extend(tech_results)
                
                print(f"Processed {tech_name}: {len(tech_results)} records")
                
            except Exception as e:
                warnings.warn(f"Error processing technology '{tech_name}': {e}")
                continue
        
        # Aggregate all results
        final_df = self._aggregate_data(all_results)
        
        # Report unknown units
        if self.unknown_units:
            print(f"\nWarning: Unknown units encountered: {self.unknown_units}")
            print("These values were used as-is without conversion.")
        
        return final_df
    
    def save_results(self, df: pd.DataFrame, output_path: str):
        """Save results to CSV file."""
        df.to_csv(output_path, index=False)
        print(f"Results saved to: {output_path}")
    
    def validate_output_format(self, df: pd.DataFrame) -> bool:
        """
        Validate that output DataFrame matches expected format.
        
        Args:
            df: Output DataFrame
            
        Returns:
            True if format is valid, False otherwise
        """
        expected_columns = ['year', 'stock', 'sector', 'fuel', 'end_use', 
                           'technology', 'state', 'metric', 'scenario']
        
        if list(df.columns) != expected_columns:
            print(f"Column order mismatch. Expected: {expected_columns}")
            print(f"Got: {list(df.columns)}")
            return False
        
        # Check for required values
        if df['metric'].nunique() > 1 or df['metric'].iloc[0] != 'stock':
            print("Metric column should only contain 'stock'")
            return False
        
        valid_scenarios = {'bau', 'high'}
        invalid_scenarios = set(df['scenario'].unique()) - valid_scenarios
        if invalid_scenarios:
            print(f"Invalid scenario values: {invalid_scenarios}")
            return False
        
        valid_sectors = {'resid', 'comm'}
        invalid_sectors = set(df['sector'].unique()) - valid_sectors
        if invalid_sectors:
            print(f"Invalid sector values: {invalid_sectors}")
            return False
        
        return True


def main():
    """Main function to run the parser. please contact Bilal if you need the
    latest updates on the data sources for testing purposes as they are ignored in .gitignore"""

    # File paths - JSON files are in parent directory, output in utils/
    base_path = Path(__file__).parent.parent  # Go up to BICEP root directory
    utils_path = Path(__file__).parent
    high_json_path = base_path / "high.json"
    stated_json_path = base_path / "stated.json" 
    tech_map_path = base_path / "technology_map.csv"
    output_path = utils_path / "aggregated_technology_stock_data.csv"
    
    # Initialize parser
    parser = TechnologyStockParser(
        str(high_json_path),
        str(stated_json_path),
        str(tech_map_path)
    )
    
    # Parse all technologies
    print("Starting technology stock data parsing...")
    result_df = parser.parse_all_technologies()
    
    # Validate output format
    if parser.validate_output_format(result_df):
        print("Output format validation: PASSED")
    else:
        print("Output format validation: FAILED")
    
    # Save results
    parser.save_results(result_df, str(output_path))
    
    # Print summary statistics
    print(f"\nSummary Statistics:")
    print(f"Total records: {len(result_df)}")
    print(f"Technologies: {result_df['technology'].nunique()}")
    print(f"Years: {result_df['year'].min()} - {result_df['year'].max()}")
    print(f"States: {result_df['state'].nunique()}")
    print(f"Sectors: {result_df['sector'].unique()}")
    print(f"Scenarios: {result_df['scenario'].unique()}")
    
    return result_df


if __name__ == "__main__":
    result = main()
