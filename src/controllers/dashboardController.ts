import { Request, Response } from 'express';
import { analyticsService } from '@/services/analyticsService';
import { getAzureService } from '@/services/azureService';
import { logger } from '@/utils/logger';
import { config } from '@/utils/config';
import { SalesData } from '@/types/data';

export class DashboardController {
  /**
   * Get dashboard overview data
   */
  async getDashboardOverview(req: Request, res: Response) {
    try {
      logger.info('Getting dashboard overview');
      const filters = this.parseFilters(req);
      // Inject RLS scopes from middleware
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const csvData = await analyticsService.getFilteredData(filters);

      // Calculate overview metrics using transformed fields
      const totalRevenue = csvData.reduce((sum: number, row: any) => sum + (row.gSales || 0), 0);
      const totalCost = csvData.reduce((sum: number, row: any) => sum + (row['Group Cost'] || 0), 0);
      const grossProfit = csvData.reduce((sum: number, row: any) => sum + (row.fGP || 0), 0);
      const margin = totalRevenue > 0 ? (grossProfit / totalRevenue) * 100 : 0;
      const totalVolume = csvData.reduce((sum: number, row: any) => sum + (row.Cases || 0), 0);

      // Compute growth from trend analysis (last change percent)
      let growth = { revenue: 0, volume: 0, margin: 0 };
      try {
        const revenueTrend = await analyticsService.getTrendAnalysis(filters, 'gSales');
        const volumeTrend = await analyticsService.getTrendAnalysis(filters, 'Cases');
        const lastRev = revenueTrend[revenueTrend.length - 1];
        const lastVol = volumeTrend[volumeTrend.length - 1];
        growth.revenue = lastRev?.changePercent || 0;
        growth.volume = lastVol?.changePercent || 0;

        // Margin trend: compute last two months margin
        const byMonth: Record<string, { sales: number; gp: number }> = {};
        for (const r of csvData) {
          const m = r['Month Name'];
          if (!m) continue;
          if (!byMonth[m]) byMonth[m] = { sales: 0, gp: 0 };
          byMonth[m].sales += r.gSales || 0;
          byMonth[m].gp += r.fGP || 0;
        }
        const months = Object.keys(byMonth).sort();
        if (months.length >= 2) {
          const lastM = months[months.length - 1];
          const prevM = months[months.length - 2];
          const lastMargin = byMonth[lastM].sales > 0 ? (byMonth[lastM].gp / byMonth[lastM].sales) * 100 : 0;
          const prevMargin = byMonth[prevM].sales > 0 ? (byMonth[prevM].gp / byMonth[prevM].sales) * 100 : 0;
          const diff = lastMargin - prevMargin;
          growth.margin = prevMargin !== 0 ? (diff / Math.abs(prevMargin)) * 100 : 0;
        }
      } catch (e) {
        // ignore growth errors
      }

      res.json({
        success: true,
        data: {
          overview: {
            totalRevenue,
            totalCost,
            grossProfit,
            margin,
            totalVolume,
            growth
          }
        }
      });
    } catch (error) {
      logger.error('Error getting dashboard overview:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'DASHBOARD_ERROR',
          message: 'Failed to get dashboard overview',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
      });
    }
  }

  /**
   * Get business areas performance
   */
  async getBusinessAreas(req: Request, res: Response) {
    try {
      logger.info('Getting business areas performance');

      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const businessAreas = await analyticsService.getBusinessAreaPerformance(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: businessAreas });
    } catch (error) {
      logger.error('Error getting business areas:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'BUSINESS_AREAS_ERROR',
          message: 'Failed to get business areas data',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
      });
    }
  }

  /**
   * Get channels performance
   */
  async getChannels(req: Request, res: Response) {
    try {
      logger.info('Getting channels performance');

      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const channels = await analyticsService.getChannelPerformance(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: channels });
    } catch (error) {
      logger.error('Error getting channels:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'CHANNELS_ERROR',
          message: 'Failed to get channels data',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
      });
    }
  }

  /**
   * Get performance data
   */
  async getPerformanceData(req: Request, res: Response) {
    try {
      logger.info('Getting performance data');
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const csvData = await analyticsService.getFilteredData(filters);

      // Top 20 rows by revenue using transformed fields
      const performanceData = csvData
        .map((row: any) => {
          const revenue = row.gSales || 0;
          const cost = row['Group Cost'] || 0;
          const gp = row.fGP || 0;
          return {
            businessArea: row.Business || 'Unknown',
            channel: row.Channel || 'Unknown',
            brand: row.Brand || 'Unknown',
            category: row.Category || 'Unknown',
            subCategory: row['Sub-Cat'] || 'Unknown',
            customer: row.Customer || 'Unknown',
            revenue,
            cost,
            volume: row.Cases || 0,
            margin: revenue > 0 ? (gp / revenue) * 100 : 0
          };
        })
        .sort((a: any, b: any) => b.revenue - a.revenue)
        .slice(0, 20);

      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({
        success: true,
        data: performanceData
      });
    } catch (error) {
      logger.error('Error getting performance data:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'PERFORMANCE_ERROR',
          message: 'Failed to get performance data',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
      });
    }
  }

  /**
   * Get filter options (years, months, business areas, channels)
   */
  async getFilterOptions(req: Request, res: Response) {
    try {
      const { year, businessArea } = req.query;
      
      let filters: any = {};
      if (year) filters.year = parseInt(year as string);
      if (businessArea) filters.businessArea = businessArea as string;

      const filterOptions = await analyticsService.getFilterOptions(filters);
      
      res.json({
        success: true,
        data: filterOptions
      });
    } catch (error) {
      logger.error('Error getting filter options:', error);
      res.status(500).json({
        success: false,
        message: 'Failed to get filter options'
      });
    }
  }

  /**
   * Get data health information
   */
  async getDataHealth(req: Request, res: Response) {
    try {
      logger.info('Getting data health');

      const azureService = getAzureService();
      const csvData = await azureService.fetchCSVData();

      // Calculate data health metrics
      const totalRows = csvData.length;
      const rowsWithRevenue = csvData.filter((row: any) => (row.gSales || 0) > 0).length;
      const rowsWithCost = csvData.filter((row: any) => (row['Group Cost'] || 0) > 0).length;
      const rowsWithVolume = csvData.filter((row: any) => (row.Cases || 0) > 0).length;

      const dataHealth = {
        totalRows,
        completeness: {
          revenue: totalRows > 0 ? (rowsWithRevenue / totalRows) * 100 : 0,
          cost: totalRows > 0 ? (rowsWithCost / totalRows) * 100 : 0,
          volume: totalRows > 0 ? (rowsWithVolume / totalRows) * 100 : 0
        },
        lastUpdated: new Date().toISOString(),
        status: 'healthy'
      };

      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({
        success: true,
        data: dataHealth
      });
    } catch (error) {
      logger.error('Error getting data health:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'DATA_HEALTH_ERROR',
          message: 'Failed to get data health',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
      });
    }
  }

  /**
   * Get trend analysis
   */
  async getTrend(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const metric = (req.query.metric as string) || 'gSales';
      const trends = await analyticsService.getTrendAnalysis(filters, metric);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: trends });
    } catch (error) {
      logger.error('Error getting trend analysis:', error);
      res.status(500).json({ success: false, error: { code: 'TREND_ERROR', message: 'Failed to get trend analysis' } });
    }
  }

  /**
   * Get top performers
   */
  async getTopPerformers(req: Request, res: Response) {
    try {
      logger.info('Getting top performers');

      const filters = this.parseFilters(req);
      const metric = (req.query.metric as string) || 'gSales';
      const dimension = (req.query.dimension as string) || 'Brand';
      const limit = Math.min(Number(req.query.limit) || config.topNDefaultLimit, 100); // Max 100
      const offset = Math.max(Number(req.query.offset) || 0, 0);

      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });

      const topPerformers = await analyticsService.getTopPerformers(filters, metric, limit, dimension as keyof SalesData, offset);

      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({
        success: true,
        data: topPerformers
      });
    } catch (error) {
      logger.error('Error getting top performers:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'TOP_PERFORMERS_ERROR',
          message: 'Failed to get top performers',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
      });
    }
  }

  /**
   * Get risk analysis
   */
  async getRisk(req: Request, res: Response) {
    try {
      logger.info('Getting risk analysis');

      const filters = this.parseFilters(req);
      const dimension = (req.query.dimension as string) || 'Brand';
      const limit = Math.min(Number(req.query.limit) || config.topNDefaultLimit, 100); // Max 100
      const offset = Math.max(Number(req.query.offset) || 0, 0);

      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });

      const risks = await analyticsService.getRiskAnalysis(filters, dimension as keyof SalesData, limit, offset);

      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ 
        success: true, 
        data: risks 
      });
    } catch (error) {
      logger.error('Error getting risk analysis:', error);
      res.status(500).json({ 
        success: false, 
        error: { 
          code: 'RISK_ERROR', 
          message: 'Failed to get risk analysis',
          details: error instanceof Error ? error.message : 'Unknown error'
        } 
      });
    }
  }

  /**
   * Get category performance
   */
  async getCategoryPerformance(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const categories = await analyticsService.getCategoryPerformance(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: categories });
    } catch (error) {
      logger.error('Error getting category performance:', error);
      res.status(500).json({ success: false, error: { code: 'CATEGORY_PERF_ERROR', message: 'Failed to get category performance' } });
    }
  }

  /**
   * Get variance analysis
   */
  async getVariance(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const comparison = (req.query.comparison as string) || 'LYTD';
      const variance = await analyticsService.getVariance(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: variance });
    } catch (error) {
      logger.error('Error getting variance analysis:', error);
      res.status(500).json({ success: false, error: { code: 'VARIANCE_ERROR', message: 'Failed to get variance analysis' } });
    }
  }

  /**
   * Get business area detailed metrics
   */
  async getBusinessAreaDetailedMetrics(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const detailedMetrics = await analyticsService.getBusinessAreaDetailedMetrics(filters);
      const meta = getAzureService().getLastFetchMeta();
      
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      
      res.json({ success: true, data: detailedMetrics });
    } catch (error) {
      logger.error('Error getting business area detailed metrics:', error);
      res.status(500).json({ 
        success: false, 
        error: { 
          code: 'BUSINESS_AREA_DETAILED_ERROR', 
          message: 'Failed to get business area detailed metrics' 
        } 
      });
    }
  }

  /**
   * Get aggregated metrics
   */
  async getAggregates(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const aggregates = await analyticsService.getAggregatedData(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: aggregates });
    } catch (error) {
      logger.error('Error getting aggregates:', error);
      res.status(500).json({ success: false, error: { code: 'AGGREGATES_ERROR', message: 'Failed to get aggregates' } });
    }
  }

  /**
   * Get sub-category performance
   */
  async getSubCategories(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const subs = await analyticsService.getSubCategoryPerformance(filters);
      res.json({ success: true, data: subs });
    } catch (error) {
      logger.error('Error getting sub-category performance:', error);
      res.status(500).json({ success: false, error: { code: 'SUBCATEGORY_PERF_ERROR', message: 'Failed to get sub-category performance' } });
    }
  }

  /**
   * Get customer performance data
   */
  async getCustomerPerformance(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const customerData = await analyticsService.getCustomerPerformance(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: customerData });
    } catch (error) {
      logger.error('Error getting customer performance:', error);
      res.status(500).json({ success: false, error: { code: 'CUSTOMER_PERF_ERROR', message: 'Failed to get customer performance' } });
    }
  }

  /**
   * Get customer overview cards data
   */
  async getCustomerOverview(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const overviewData = await analyticsService.getCustomerOverview(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: overviewData });
    } catch (error) {
      logger.error('Error getting customer overview:', error);
      res.status(500).json({ success: false, error: { code: 'CUSTOMER_OVERVIEW_ERROR', message: 'Failed to get customer overview' } });
    }
  }

  /**
   * Get top customers data
   */
  async getTopCustomers(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const topCustomers = await analyticsService.getTopCustomers(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: topCustomers });
    } catch (error) {
      logger.error('Error getting top customers:', error);
      res.status(500).json({ success: false, error: { code: 'TOP_CUSTOMERS_ERROR', message: 'Failed to get top customers' } });
    }
  }

  /**
   * Get customer channels analysis
   */
  async getCustomerChannels(req: Request, res: Response) {
    try {
      const filters = this.parseFilters(req);
      const user = (req as any).user || {};
      Object.assign(filters, {
        allowedBusinessAreas: user.allowedBusinessAreas,
        allowedChannels: user.allowedChannels,
        allowedBrands: user.allowedBrands,
        allowedCustomers: user.allowedCustomers,
      });
      const channelData = await analyticsService.getCustomerChannels(filters);
      const meta = getAzureService().getLastFetchMeta();
      res.setHeader('x-data-source', meta.source);
      res.setHeader('x-row-count', String(meta.rowCount));
      res.setHeader('x-last-updated', meta.lastUpdated);
      res.json({ success: true, data: channelData });
    } catch (error) {
      logger.error('Error getting customer channels:', error);
      res.status(500).json({ success: false, error: { code: 'CUSTOMER_CHANNELS_ERROR', message: 'Failed to get customer channels' } });
    }
  }

  /**
   * Parse filters from query
   */
  private parseFilters(req: Request) {
    const year = req.query.year ? parseInt(req.query.year as string, 10) : undefined;
    const period = req.query.period ? String(req.query.period) : undefined;
    const month = req.query.month ? String(req.query.month) : undefined;
    const businessArea = req.query.businessArea ? String(req.query.businessArea) : undefined;
    const brand = req.query.brand ? String(req.query.brand) : undefined;
    const category = req.query.category ? String(req.query.category) : undefined;
    const subCategory = req.query.subCategory ? String(req.query.subCategory) : undefined;
    const channel = req.query.channel ? String(req.query.channel) : undefined;
    const customer = req.query.customer ? String(req.query.customer) : undefined;
    return { year, period, month, businessArea, brand, category, subCategory, channel, customer };
  }
}

export const dashboardController = new DashboardController();
