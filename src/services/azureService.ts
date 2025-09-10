import { BlobServiceClient } from '@azure/storage-blob';
import csv from 'csv-parser';
import { Readable } from 'stream';
import { SalesData } from '@/types/data';
import { logger } from '@/utils/logger';
import { cacheService } from '@/services/cacheService';
import Joi from 'joi';

export class AzureService {
  private blobServiceClient: BlobServiceClient;
  private containerName: string;
  private blobFolder: string;
  private csvFileName: string;
  private lastFetchMeta: { source: 'azure' | 'cache'; rowCount: number; lastUpdated: string } = {
    source: 'azure',
    rowCount: 0,
    lastUpdated: ''
  };
  private rowSchema = Joi.object({
    Year: Joi.alternatives(Joi.string().regex(/^\d{4}$/), Joi.number().integer().min(2000).max(2100)).required(),
    'Month Name': Joi.string().required(), // Allow any month name format
    Business: Joi.string().allow('').optional(),
    Channel: Joi.string().allow('').optional(),
    Brand: Joi.string().allow('').optional(),
    Category: Joi.string().allow('').optional(),
    Customer: Joi.string().allow('').optional(),
    gSales: Joi.alternatives(Joi.string(), Joi.number(), Joi.allow(null)).optional(),
    fGP: Joi.alternatives(Joi.string(), Joi.number(), Joi.allow(null)).optional(),
    'Group Cost': Joi.alternatives(Joi.string(), Joi.number(), Joi.allow(null)).optional(),
    Cases: Joi.alternatives(Joi.string(), Joi.number(), Joi.allow(null)).optional()
  }).unknown(true);

  constructor() {
    // Hardcoded Azure credentials for debugging (temporary)
    const connectionString = 'DefaultEndpointsProtocol=https;AccountName=kineticadbms;AccountKey=JfMzO69p3Ip+Sz+YkXxp7sHxZw0O/JunSaS5qKnSSQnxk1lPhwiQwnGyyJif7sGB01l9amAdvU/t+ASthIK/ZQ==;EndpointSuffix=core.windows.net';
    this.blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    this.containerName = 'thrive-worklytics';
    this.blobFolder = 'Biz-Pulse';
    // this.csvFileName = 'Front Office Flash - YTD.csv';
    this.csvFileName = 'yearly_data.csv';

    logger.info('Azure Service initialized', {
      containerName: this.containerName,
      blobFolder: this.blobFolder,
      csvFileName: this.csvFileName,
      accountName: this.blobServiceClient.accountName
    });
  }

  /**
   * Fetch CSV data from Azure Blob Storage
   */
  async fetchCSVData(): Promise<SalesData[]> {
    const cacheKey = 'csv_data';
    
    // Check cache first
    const cachedData = await cacheService.get(cacheKey);
    if (cachedData && Array.isArray(cachedData)) {
      logger.info('Returning cached CSV data');
      this.lastFetchMeta = {
        source: 'cache',
        rowCount: (cachedData as SalesData[]).length,
        lastUpdated: new Date().toISOString()
      };
      return cachedData as SalesData[];
    }

    try {
      logger.info('Fetching CSV data from Azure Blob Storage');
      
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const blobPath = `${this.blobFolder}/${this.csvFileName}`;
      const blobClient = containerClient.getBlobClient(blobPath);

      // Check if blob exists
      logger.info(`Checking if blob exists: ${blobPath}`);
      const exists = await blobClient.exists();
      if (!exists) {
        throw new Error(`CSV file not found: ${blobPath}`);
      }
      logger.info('Blob exists, proceeding to download');

      // Download blob
      const downloadResponse = await blobClient.download();
      if (!downloadResponse.readableStreamBody) {
        throw new Error('Failed to download CSV file');
      }

      // Parse CSV data
      const data: SalesData[] = [];
      const stream = downloadResponse.readableStreamBody as Readable;
      
      return new Promise((resolve, reject) => {
        stream
          .pipe(csv())
          .on('data', (row: any) => {
            // Debug: Log first few raw rows to see the data structure
            if (data.length < 3) {
              console.log(`\n--- Raw CSV Row ${data.length + 1} ---`);
              console.log('Raw row:', row);
              console.log('Year field:', row.Year, 'Type:', typeof row.Year);
            }
            
            // Transform and validate data
            const { error } = this.rowSchema.validate(row, { abortEarly: false });
            if (error) {
              logger.warn('Row validation failed; skipping row', { details: error.details?.map(d => d.message).slice(0,3) });
              return;
            }
            const transformedRow = this.transformCSVRow(row);
            if (transformedRow) {
              data.push(transformedRow);
            }
          })
          .on('end', async () => {
            logger.info(`Successfully parsed ${data.length} rows from CSV`);
            
            // Cache the data
            await cacheService.set(cacheKey, data, 3600); // Cache for 1 hour
            this.lastFetchMeta = {
              source: 'azure',
              rowCount: data.length,
              lastUpdated: new Date().toISOString()
            };
            
            resolve(data);
          })
          .on('error', (error: Error) => {
            logger.error('Error parsing CSV:', error);
            reject(error);
          });
      });

    } catch (error) {
      logger.error('Error fetching CSV data from Azure:', error);
      throw error;
    }
  }

  getLastFetchMeta(): { source: 'azure' | 'cache'; rowCount: number; lastUpdated: string } {
    return this.lastFetchMeta;
  }

  /**
   * Transform CSV row to typed data structure
   */
  private transformCSVRow(row: any): SalesData | null {
    try {
      // Handle missing or invalid data
      if (!row || !row.Year || !row['Month Name']) {
        return null;
      }

      // Debug: Check Year field conversion
      const yearValue = parseInt(row.Year);
      if (isNaN(yearValue) || yearValue === 0) {
        console.log(`⚠️ Invalid Year field: "${row.Year}" -> ${yearValue}`);
      }

      return {
        // Time dimensions
        Year: yearValue || 0,
        'Month Name': row['Month Name'] || '',
        
        // Product dimensions
        'Brand Type Name': row['Brand Type Name'] || '',
        'P+L Brand': row['P+L Brand'] || '',
        'P+L Category': row['P+L Category'] || '',
        'SubCat Name': row['SubCat Name'] || '',
        'Attribute Name': row['Attribute Name'] || '',
        'SKU Channel Name': row['SKU Channel Name'] || '',
        Brand: row.Brand || '',
        Category: row.Category || '',
        'Sub-Cat': row['Sub-Cat'] || '',
        'Board Category': row['Board Category'] || '',
        
        // Customer dimensions
        'P+L Cust. Grp': row['P+L Cust. Grp'] || '',
        Business: row.Business || '',
        Channel: row.Channel || '',
        Customer: row.Customer || '',
        'CD': row['CD'] || '',
        'UK Customer': row['UK Customer'] || '',
        'NI Customer': row['NI Customer'] || '',
        'SKU Channel': row['SKU Channel'] || '',
        'Business (created for purpose of vlookup in \'Total Brands\' tab - col C': row['Business (created for purpose of vlookup in \'Total Brands\' tab - col C'] || '',
        
        // Metrics - convert to numbers and handle invalid values
        Cases: this.parseNumber(row.Cases),
        gSales: this.parseNumber(row.gSales),
        'Price Downs': this.parseNumber(row['Price Downs']),
        'Perm. Disc.': this.parseNumber(row['Perm. Disc.']),
        'Group Cost': this.parseNumber(row['Group Cost']),
        LTA: this.parseNumber(row.LTA),
        fGP: this.parseNumber(row.fGP),
        'Avg Cost': this.parseNumber(row['Avg Cost']),
        
        // Computed fields
        ProdConcat: row.ProdConcat || ''
      };
    } catch (error) {
      logger.error('Error transforming CSV row:', error, row);
      return null;
    }
  }

  /**
   * Parse number safely, handling various formats
   */
  private parseNumber(value: any): number {
    if (value === null || value === undefined || value === '') {
      return 0;
    }
    
    // Remove currency symbols, commas, and spaces
    const cleaned = String(value).replace(/[€£$,\s]/g, '');
    const parsed = parseFloat(cleaned);
    
    return isNaN(parsed) ? 0 : parsed;
  }

  /**
   * Get available columns from CSV (for dynamic column support)
   */
  async getAvailableColumns(): Promise<string[]> {
    try {
      const data = await this.fetchCSVData();
      if (data.length === 0) {
        return [];
      }
      
      return Object.keys(data[0]);
    } catch (error) {
      logger.error('Error getting available columns:', error);
      return [];
    }
  }

  /**
   * Check if CSV file has been updated
   */
  async checkForUpdates(): Promise<boolean> {
    try {
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      const blobPath = `${this.blobFolder}/${this.csvFileName}`;
      const blobClient = containerClient.getBlobClient(blobPath);

      const properties = await blobClient.getProperties();
      const lastModified = properties.lastModified;
      
      // Check if file has been modified since last cache
      const cacheKey = 'csv_last_modified';
      const cachedLastModified = await cacheService.get(cacheKey);
      
      if (!cachedLastModified || (typeof cachedLastModified === 'number' && cachedLastModified < (lastModified?.getTime() || 0))) {
        // Clear cache and update last modified
        await cacheService.delete('csv_data');
        await cacheService.set(cacheKey, lastModified?.getTime(), 86400); // Cache for 24 hours
        return true;
      }
      
      return false;
    } catch (error) {
      logger.error('Error checking for CSV updates:', error);
      return false;
    }
  }

  /**
   * Test Azure connection without accessing specific file
   */
  async testConnection(): Promise<boolean> {
    try {
      logger.info('Testing Azure connection...');
      const containerClient = this.blobServiceClient.getContainerClient(this.containerName);
      // Try minimal list to validate auth without requiring a specific blob
      const iter = containerClient.listBlobsFlat().byPage({ maxPageSize: 1 });
      const page = await iter.next();
      const ok = !page.done; // if we got a page (even empty), auth succeeded
      if (ok) {
        logger.info('Azure connection successful');
        return true;
      }
      logger.warn('Azure connection test returned no pages');
      return false;
    } catch (error) {
      logger.error('Azure connection test failed:', error);
      return false;
    }
  }

  /**
   * Get data summary statistics
   */
  async getDataSummary(): Promise<{
    totalRows: number;
    dateRange: { start: string; end: string };
    businessAreas: string[];
    channels: string[];
    lastUpdated: string;
  }> {
    try {
      const data = await this.fetchCSVData();
      
      const years = [...new Set(data.map(row => row.Year))].sort();
      const businessAreas = [...new Set(data.map(row => row.Business))].filter(Boolean);
      const channels = [...new Set(data.map(row => row.Channel))].filter(Boolean);
      
      return {
        totalRows: data.length,
        dateRange: {
          start: `${Math.min(...years)}`,
          end: `${Math.max(...years)}`
        },
        businessAreas,
        channels,
        lastUpdated: new Date().toISOString()
      };
    } catch (error) {
      logger.error('Error getting data summary:', error);
      throw error;
    }
  }
}

// Lazy initialization to ensure environment variables are loaded
let _azureService: AzureService | null = null;

export const getAzureService = (): AzureService => {
  if (!_azureService) {
    _azureService = new AzureService();
  }
  return _azureService;
};
