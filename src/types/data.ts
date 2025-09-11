// Core data structure based on CSV columns
export interface SalesData {
  // Time dimensions
  Year: number;
  'Month Name': string;
  
  // Product dimensions
  'Brand Type Name': string;
  'P+L Brand': string;
  'P+L Category': string;
  'SubCat Name': string;
  'Attribute Name': string;
  'SKU Channel Name': string;
  Brand: string;
  Category: string;
  'Sub-Cat': string;
  'Board Category': string;
  
  // Customer dimensions
  'P+L Cust. Grp': string;
  Business: string;
  Channel: string;
  Customer: string;
  'CD': string;
  'UK Customer': string;
  'NI Customer': string;
  'SKU Channel': string;
  'Business (created for purpose of vlookup in \'Total Brands\' tab - col C': string;
  
  // Metrics
  Cases: number;
  gSales: number;
  'Price Downs': number;
  'Perm. Disc.': number;
  'Group Cost': number;
  LTA: number;
  fGP: number;
  'Avg Cost': number;
  
  // Computed fields
  ProdConcat?: string;
}

// Filter interfaces
export interface DataFilters {
  year?: number;
  month?: string;
  businessArea?: string;
  brand?: string;
  category?: string;
  subCategory?: string;
  channel?: string;
  customer?: string;
  period?: string;
  limit?: number;
  offset?: number;
  // RLS filters
  allowedBusinessAreas?: string[];
  allowedChannels?: string[];
  allowedBrands?: string[];
  allowedCustomers?: string[];
  // Internal flags
  skipYearFilter?: boolean;
}

// Aggregated data interfaces
export interface AggregatedData {
  totalRevenue: number;
  totalCases: number;
  totalMargin: number;
  avgMargin: number;
  growthRate: number;
  topPerformers: TopPerformer[];
  riskItems: RiskItem[];
  uniqueCustomers?: number;
  uniqueBrands?: number;
  uniqueCategories?: number;
}

export interface TopPerformer {
  name: string;
  value: number;
  metric: string;
  growth: number;
  category: string;
}

export interface RiskItem {
  name: string;
  value: number;
  riskLevel: 'high' | 'medium' | 'low';
  reason: string;
  trend: number;
}

// Pagination interfaces
export interface PaginationParams {
  limit?: number;
  offset?: number;
}

export interface PaginatedResponse<T> {
  data: T[];
  pagination: {
    total: number;
    limit: number;
    offset: number;
    hasMore: boolean;
    totalPages: number;
    currentPage: number;
  };
}

// Business dimensions
export interface BusinessArea {
  name: string;
  brands: string[];
  revenue: number;
  margin: number;
  growth: number;
  color: string;
}

export interface Channel {
  name: string;
  customers: string[];
  region: string;
  revenue: number;
  growth: number;
  customerCount: number;
}

export interface Customer {
  name: string;
  channel: string;
  revenue: number;
  margin: number;
  growth: number;
  orders: number;
  avgOrderValue: number;
  status: 'growing' | 'stable' | 'declining';
}

// API Response interfaces
export interface ApiResponse<T> {
  success: boolean;
  data: T;
  message?: string;
  pagination?: {
    page: number;
    limit: number;
    total: number;
    totalPages: number;
  };
}

export interface ErrorResponse {
  success: false;
  error: {
    code: string;
    message: string;
    details?: any;
  };
}

// Cache interfaces
export interface CacheConfig {
  ttl: number;
  key: string;
  data: any;
}

// Analytics interfaces
export interface VarianceAnalysis {
  totalVariance: number;
  volumeVariance: number;
  priceVariance: number;
  costVariance: number;
  mixVariance: number;
  period: string;
  comparison: string;
}

export interface TrendAnalysis {
  period: string;
  trend: 'up' | 'down' | 'stable';
  value: number;
  change: number;
  changePercent: number;
}






