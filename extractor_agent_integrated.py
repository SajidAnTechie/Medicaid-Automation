import pandas as pd
import os
from pathlib import Path
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MedicaidExtractorAgent:
    """
    Automated extractor agent for Medicaid fee schedule data.
    Processes raw CSV files and extracts structured information.
    """
    
    def __init__(self, input_file, output_dir=None):
        self.input_file = input_file
        self.output_dir = output_dir or os.path.dirname(input_file)
        self.data = None
        self.extracted_data = None
        
    def load_data(self):
        """Load the raw CSV data"""
        try:
            logger.info(f"Loading data from {self.input_file}")
            self.data = pd.read_csv(self.input_file)
            logger.info(f"Successfully loaded {len(self.data)} rows")
            return True
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            return False
    
    def extract_procedure_codes(self):
        """Extract and clean procedure codes"""
        if self.data is None:
            logger.error("No data loaded. Call load_data() first.")
            return None
        
        logger.info("Extracting procedure codes...")
        
        # Identify procedure code column (CPT, HCPCS, etc.)
        code_columns = [col for col in self.data.columns 
                       if any(term in col.lower() for term in ['code', 'cpt', 'hcpcs', 'procedure'])]
        
        if code_columns:
            logger.info(f"Found code columns: {code_columns}")
            return self.data[code_columns[0]].dropna().unique()
        
        return None
    
    def extract_fees(self):
        """Extract and clean fee information"""
        if self.data is None:
            logger.error("No data loaded. Call load_data() first.")
            return None
        
        logger.info("Extracting fee information...")
        
        # Identify fee/rate columns
        fee_columns = [col for col in self.data.columns 
                      if any(term in col.lower() for term in ['fee', 'rate', 'amount', 'price', 'payment'])]
        
        if fee_columns:
            logger.info(f"Found fee columns: {fee_columns}")
            return fee_columns
        
        return None
    
    def clean_and_structure(self):
        """Clean and structure the extracted data"""
        if self.data is None:
            logger.error("No data loaded. Call load_data() first.")
            return None
        
        logger.info("Cleaning and structuring data...")
        
        # Create structured output
        self.extracted_data = self.data.copy()
        
        # Remove completely empty rows
        self.extracted_data = self.extracted_data.dropna(how='all')
        
        # Clean column names
        self.extracted_data.columns = [col.strip().replace('\n', ' ').replace('  ', ' ') 
                                       for col in self.extracted_data.columns]
        
        logger.info(f"Structured data contains {len(self.extracted_data)} rows and {len(self.extracted_data.columns)} columns")
        
        return self.extracted_data
    
    def save_extracted_data(self, filename=None):
        """Save the extracted and cleaned data"""
        if self.extracted_data is None:
            logger.error("No extracted data to save. Run extract process first.")
            return None
        
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"extracted_medicaid_data_{timestamp}.csv"
        
        output_path = os.path.join(self.output_dir, filename)
        
        try:
            self.extracted_data.to_csv(output_path, index=False)
            logger.info(f"Extracted data saved to {output_path}")
            return output_path
        except Exception as e:
            logger.error(f"Error saving data: {e}")
            return None
    
    def generate_report(self):
        """Generate a summary report of the extraction"""
        if self.extracted_data is None:
            logger.error("No extracted data available.")
            return None
        
        report = {
            'total_rows': len(self.extracted_data),
            'total_columns': len(self.extracted_data.columns),
            'columns': list(self.extracted_data.columns),
            'missing_data': self.extracted_data.isnull().sum().to_dict(),
            'data_types': self.extracted_data.dtypes.astype(str).to_dict()
        }
        
        logger.info("Extraction Report:")
        logger.info(f"  Total Rows: {report['total_rows']}")
        logger.info(f"  Total Columns: {report['total_columns']}")
        logger.info(f"  Columns: {report['columns']}")
        
        return report
    
    def run_auto_extraction(self):
        """Run the complete extraction process automatically"""
        logger.info("=" * 60)
        logger.info("Starting Automated Extraction Process")
        logger.info("=" * 60)
        
        # Step 1: Load data
        if not self.load_data():
            logger.error("Extraction failed at loading stage")
            return False
        
        # Step 2: Extract procedure codes
        codes = self.extract_procedure_codes()
        if codes is not None:
            logger.info(f"Extracted {len(codes)} unique procedure codes")
        
        # Step 3: Extract fees
        fee_cols = self.extract_fees()
        if fee_cols:
            logger.info(f"Identified {len(fee_cols)} fee-related columns")
        
        # Step 4: Clean and structure
        structured_data = self.clean_and_structure()
        if structured_data is None:
            logger.error("Extraction failed at cleaning stage")
            return False
        
        # Step 5: Generate report
        report = self.generate_report()
        
        # Step 6: Save extracted data
        output_file = self.save_extracted_data()
        if output_file:
            logger.info(f"Extraction completed successfully!")
            logger.info(f"Output saved to: {output_file}")
            return True
        
        return False


def main():
    """Main function to run the extractor agent"""
    # Path to the Alaska raw data
    input_file = "/Users/sudipkunwar/Medicaid_Feeschedule/alaska_raw_data.csv"
    
    # Create extractor agent instance
    agent = MedicaidExtractorAgent(input_file)
    
    # Run automatic extraction
    success = agent.run_auto_extraction()
    
    if success:
        print("\n✓ Extraction completed successfully!")
        print(f"✓ Check the output directory for results: {agent.output_dir}")
    else:
        print("\n✗ Extraction failed. Check logs for details.")


if __name__ == "__main__":
    main()
