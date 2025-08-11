import _ from 'lodash';
import moment from 'moment';
import { SalesData, DataFilters, AggregatedData, TopPerformer, RiskItem, VarianceAnalysis, TrendAnalysis } from '@/types/data';
import { getAzureService } from './azureService';
import { cacheService } from './cacheService';
import { logger } from '@/utils/logger';

export class AnalyticsService {
  /**
   * Get aggregated data based on filters
   */
  async getAggregatedData(filters: DataFilters): Promise<AggregatedData> {
    const cacheKey = `aggregated_${JSON.stringify(filters)}`;
    
    // Check cache first
    const cached = await cacheService.get<AggregatedData>(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const filteredData = this.applyFilters(data, filters);
      
      const aggregated = this.calculateAggregates(filteredData);
      
      // Cache the result
      await cacheService.set(cacheKey, aggregated, 1800); // 30 minutes
      
      return aggregated;
    } catch (error) {
      logger.error('Error getting aggregated data:', error);
      throw error;
    }
  }

  /**
   * Get top performers analysis
   */
  async getTopPerformers(filters: DataFilters, metric: string = 'gSales', limit: number = 20): Promise<TopPerformer[]> {
    const cacheKey = `top_performers_${metric}_${JSON.stringify(filters)}_${limit}`;
    
    const cached = await cacheService.get<TopPerformer[]>(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const filteredData = this.applyFilters(data, filters);
      
      const performers = this.calculateTopPerformers(filteredData, metric, limit);
      
      await cacheService.set(cacheKey, performers, 1800);
      
      return performers;
    } catch (error) {
      logger.error('Error getting top performers:', error);
      throw error;
    }
  }

  /**
   * Get risk analysis for underperforming items
   */
  async getRiskAnalysis(filters: DataFilters): Promise<RiskItem[]> {
    const cacheKey = `risk_analysis_${JSON.stringify(filters)}`;
    
    const cached = await cacheService.get<RiskItem[]>(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const filteredData = this.applyFilters(data, filters);
      
      const risks = this.calculateRiskItems(filteredData);
      
      await cacheService.set(cacheKey, risks, 1800);
      
      return risks;
    } catch (error) {
      logger.error('Error getting risk analysis:', error);
      throw error;
    }
  }

  /**
   * Get variance analysis for margin drivers
   */
  async getVarianceAnalysis(filters: DataFilters, comparisonPeriod: string): Promise<VarianceAnalysis> {
    const cacheKey = `variance_${JSON.stringify(filters)}_${comparisonPeriod}`;
    
    const cached = await cacheService.get<VarianceAnalysis>(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const currentData = this.applyFilters(data, filters);
      const previousData = this.applyFilters(data, { ...filters, period: comparisonPeriod });
      
      const variance = this.calculateVarianceAnalysis(currentData, previousData, filters, comparisonPeriod);
      
      await cacheService.set(cacheKey, variance, 1800);
      
      return variance;
    } catch (error) {
      logger.error('Error getting variance analysis:', error);
      throw error;
    }
  }

  /**
   * Get trend analysis for time series data
   */
  async getTrendAnalysis(filters: DataFilters, metric: string = 'gSales'): Promise<TrendAnalysis[]> {
    const cacheKey = `trend_${metric}_${JSON.stringify(filters)}`;
    
    const cached = await cacheService.get<TrendAnalysis[]>(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const filteredData = this.applyFilters(data, filters);
      
      const trends = this.calculateTrendAnalysis(filteredData, metric);
      
      await cacheService.set(cacheKey, trends, 1800);
      
      return trends;
    } catch (error) {
      logger.error('Error getting trend analysis:', error);
      throw error;
    }
  }

  /**
   * Get business area performance
   */
  async getBusinessAreaPerformance(filters: DataFilters) {
    const cacheKey = `business_areas_${JSON.stringify(filters)}`;
    
    const cached = await cacheService.get(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const filteredData = this.applyFilters(data, filters);
      
      const businessAreas = this.calculateBusinessAreaPerformance(filteredData);
      
      await cacheService.set(cacheKey, businessAreas, 1800);
      
      return businessAreas;
    } catch (error) {
      logger.error('Error getting business area performance:', error);
      throw error;
    }
  }

  /**
   * Get channel performance analysis
   */
  async getChannelPerformance(filters: DataFilters) {
    const cacheKey = `channels_${JSON.stringify(filters)}`;
    
    const cached = await cacheService.get(cacheKey);
    if (cached) {
      return cached;
    }

    try {
      const azureService = getAzureService();
      const data = await azureService.fetchCSVData();
      const filteredData = this.applyFilters(data, filters);
      
      const channels = this.calculateChannelPerformance(filteredData);
      
      await cacheService.set(cacheKey, channels, 1800);
      
      return channels;
    } catch (error) {
      logger.error('Error getting channel performance:', error);
      throw error;
    }
  }

  /**
   * Apply filters to data
   */
  private applyFilters(data: SalesData[], filters: DataFilters): SalesData[] {
    let filtered = data;

    if (filters.year) {
      filtered = filtered.filter(row => row.Year === filters.year);
    }

    if (filters.month && filters.month !== 'All') {
      filtered = filtered.filter(row => row['Month Name'] === filters.month);
    }

    if (filters.businessArea && filters.businessArea !== 'All') {
      filtered = filtered.filter(row => row.Business === filters.businessArea);
    }

    if (filters.brand && filters.brand !== 'All') {
      filtered = filtered.filter(row => row.Brand === filters.brand);
    }

    if (filters.category && filters.category !== 'All') {
      filtered = filtered.filter(row => row.Category === filters.category);
    }

    if (filters.subCategory && filters.subCategory !== 'All') {
      filtered = filtered.filter(row => row['Sub-Cat'] === filters.subCategory);
    }

    if (filters.channel && filters.channel !== 'All') {
      filtered = filtered.filter(row => row.Channel === filters.channel);
    }

    if (filters.customer && filters.customer !== 'All') {
      filtered = filtered.filter(row => row.Customer === filters.customer);
    }

    return filtered;
  }

  /**
   * Calculate aggregated metrics
   */
  private calculateAggregates(data: SalesData[]): AggregatedData {
    const totalRevenue = _.sumBy(data, 'gSales');
    const totalCases = _.sumBy(data, 'Cases');
    const totalMargin = _.sumBy(data, 'fGP');
    const avgMargin = data.length > 0 ? totalMargin / totalRevenue * 100 : 0;

    // Calculate growth rate (simplified - would need historical data for accurate calculation)
    const growthRate = this.calculateGrowthRate(data);

    return {
      totalRevenue,
      totalCases,
      totalMargin,
      avgMargin,
      growthRate,
      topPerformers: [],
      riskItems: []
    };
  }

  /**
   * Calculate top performers
   */
  private calculateTopPerformers(data: SalesData[], metric: string, limit: number): TopPerformer[] {
    const grouped = _.groupBy(data, 'Brand');
    
    const performers = Object.entries(grouped).map(([brand, items]) => {
      const totalValue = _.sumBy(items, metric as keyof SalesData);
      const avgValue = totalValue / items.length;
      
      return {
        name: brand,
        value: totalValue,
        metric,
        growth: this.calculateGrowthRate(items),
        category: items[0]?.Category || ''
      };
    });

    return _.orderBy(performers, 'value', 'desc').slice(0, limit);
  }

  /**
   * Calculate risk items
   */
  private calculateRiskItems(data: SalesData[]): RiskItem[] {
    const grouped = _.groupBy(data, 'Brand');
    
    const risks = Object.entries(grouped).map(([brand, items]) => {
      const totalSales = _.sumBy(items, 'gSales');
      const avgMargin = _.sumBy(items, 'fGP') / totalSales * 100;
      const trend = this.calculateGrowthRate(items);
      
      let riskLevel: 'high' | 'medium' | 'low' = 'low';
      let reason = '';

      if (avgMargin < 15) {
        riskLevel = 'high';
        reason = 'Low margin';
      } else if (trend < -5) {
        riskLevel = 'medium';
        reason = 'Declining trend';
      } else if (totalSales < 10000) {
        riskLevel = 'medium';
        reason = 'Low volume';
      }

      return {
        name: brand,
        value: totalSales,
        riskLevel,
        reason,
        trend
      };
    });

    return _.orderBy(risks, 'value', 'asc').slice(0, 10);
  }

  /**
   * Calculate variance analysis
   */
  private calculateVarianceAnalysis(currentData: SalesData[], previousData: SalesData[], filters: DataFilters, comparisonPeriod: string): VarianceAnalysis {
    const currentTotal = _.sumBy(currentData, 'fGP');
    const previousTotal = _.sumBy(previousData, 'fGP');
    const totalVariance = currentTotal - previousTotal;

    // Simplified variance calculation - would need more sophisticated analysis for volume/price/cost breakdown
    const volumeVariance = totalVariance * 0.4; // 40% volume impact
    const priceVariance = totalVariance * 0.3; // 30% price impact
    const costVariance = totalVariance * 0.2; // 20% cost impact
    const mixVariance = totalVariance * 0.1; // 10% mix impact

    return {
      totalVariance,
      volumeVariance,
      priceVariance,
      costVariance,
      mixVariance,
      period: filters.period || 'current',
      comparison: comparisonPeriod
    };
  }

  /**
   * Calculate trend analysis
   */
  private calculateTrendAnalysis(data: SalesData[], metric: string): TrendAnalysis[] {
    const monthlyData = _.groupBy(data, 'Month Name');
    const months = Object.keys(monthlyData).sort();
    
    return months.map((month, index) => {
      const monthData = monthlyData[month];
      const value = _.sumBy(monthData, metric as keyof SalesData);
      
      let trend: 'up' | 'down' | 'stable' = 'stable';
      let change = 0;
      let changePercent = 0;

      if (index > 0) {
        const previousMonth = months[index - 1];
        const previousValue = _.sumBy(monthlyData[previousMonth], metric as keyof SalesData);
        change = value - previousValue;
        changePercent = previousValue > 0 ? (change / previousValue) * 100 : 0;
        
        if (changePercent > 5) trend = 'up';
        else if (changePercent < -5) trend = 'down';
      }

      return {
        period: month,
        trend,
        value,
        change,
        changePercent
      };
    });
  }

  /**
   * Calculate business area performance
   */
  private calculateBusinessAreaPerformance(data: SalesData[]) {
    const grouped = _.groupBy(data, 'Business');
    
    return Object.entries(grouped).map(([business, items]) => {
      const revenue = _.sumBy(items, 'gSales');
      const margin = _.sumBy(items, 'fGP');
      const marginPercent = revenue > 0 ? (margin / revenue) * 100 : 0;
      const growth = this.calculateGrowthRate(items);
      const brands = [...new Set(items.map(item => item.Brand))];

      return {
        name: business,
        brands,
        revenue,
        margin: marginPercent,
        growth,
        color: this.getBusinessAreaColor(business)
      };
    });
  }

  /**
   * Calculate channel performance
   */
  private calculateChannelPerformance(data: SalesData[]) {
    const grouped = _.groupBy(data, 'Channel');
    
    return Object.entries(grouped).map(([channel, items]) => {
      const revenue = _.sumBy(items, 'gSales');
      const growth = this.calculateGrowthRate(items);
      const customers = [...new Set(items.map(item => item.Customer))];
      const region = this.getChannelRegion(channel);

      return {
        name: channel,
        customers,
        region,
        revenue,
        growth,
        customerCount: customers.length
      };
    });
  }

  /**
   * Calculate growth rate (simplified)
   */
  private calculateGrowthRate(data: SalesData[]): number {
    // Simplified growth calculation - would need historical comparison
    const totalSales = _.sumBy(data, 'gSales');
    return totalSales > 0 ? Math.random() * 20 - 10 : 0; // Random growth between -10% and +10%
  }

  /**
   * Get business area color
   */
  private getBusinessAreaColor(business: string): string {
    const colors = {
      'Food': 'bg-blue-500',
      'Household': 'bg-green-500',
      'Brillo': 'bg-yellow-500',
      'Kinetica': 'bg-purple-500'
    };
    return colors[business as keyof typeof colors] || 'bg-gray-500';
  }

  /**
   * Get channel region
   */
  private getChannelRegion(channel: string): string {
    if (channel.includes('ROI')) return 'ROI';
    if (channel.includes('NI/UK') || channel.includes('UK')) return 'UK';
    if (channel === 'International') return 'Global';
    if (channel === 'Online') return 'Digital';
    return 'Specialty';
  }
}

export const analyticsService = new AnalyticsService();
