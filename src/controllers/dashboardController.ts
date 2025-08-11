import { Request, Response } from 'express';
import { getAzureService } from '@/services/azureService';
import { analyticsService } from '@/services/analyticsService';
import { logger } from '@/utils/logger';

export class DashboardController {
  /**
   * Get dashboard overview data
   */
  async getDashboardOverview(req: Request, res: Response) {
    try {
      logger.info('Getting dashboard overview');

      const azureService = getAzureService();
      const csvData = await azureService.fetchCSVData();

      // Calculate overview metrics using transformed fields
      const totalRevenue = csvData.reduce((sum: number, row: any) => sum + (row.gSales || 0), 0);
      const totalCost = csvData.reduce((sum: number, row: any) => sum + (row['Group Cost'] || 0), 0);
      const grossProfit = csvData.reduce((sum: number, row: any) => sum + (row.fGP || 0), 0);
      const margin = totalRevenue > 0 ? (grossProfit / totalRevenue) * 100 : 0;
      const totalVolume = csvData.reduce((sum: number, row: any) => sum + (row.Cases || 0), 0);

      // Mock growth data for now
      const growth = {
        revenue: 12.5,
        volume: 8.3,
        margin: 2.1
      };

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

      // Use analytics service to return consistent Friday shape
      const businessAreas = await analyticsService.getBusinessAreaPerformance({});

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

      // Use analytics service to return consistent Friday shape
      const channels = await analyticsService.getChannelPerformance({});

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

      const azureService = getAzureService();
      const csvData = await azureService.fetchCSVData();

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
            customer: row.Customer || 'Unknown',
            revenue,
            cost,
            volume: row.Cases || 0,
            margin: revenue > 0 ? (gp / revenue) * 100 : 0
          };
        })
        .sort((a: any, b: any) => b.revenue - a.revenue)
        .slice(0, 20);

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
   * Get filter options
   */
  async getFilterOptions(req: Request, res: Response) {
    try {
      logger.info('Getting filter options');

      const azureService = getAzureService();
      const csvData = await azureService.fetchCSVData();

      // Extract unique values for filters
      const businessAreas = [...new Set(csvData.map((row: any) => row.Business).filter(Boolean))];
      const brands = [...new Set(csvData.map((row: any) => row.Brand).filter(Boolean))];
      const categories = [...new Set(csvData.map((row: any) => row.Category).filter(Boolean))];
      const channels = [...new Set(csvData.map((row: any) => row.Channel).filter(Boolean))];
      const customers = [...new Set(csvData.map((row: any) => row.Customer).filter(Boolean))];

      res.json({
        success: true,
        data: {
          periods: ['YTD', 'MTD', 'QTD', 'LYTD', 'LMTD', 'LQTD'],
          businessAreas,
          brands,
          categories,
          channels,
          customers
        }
      });
    } catch (error) {
      logger.error('Error getting filter options:', error);
      res.status(500).json({
        success: false,
        error: {
          code: 'FILTER_OPTIONS_ERROR',
          message: 'Failed to get filter options',
          details: error instanceof Error ? error.message : 'Unknown error'
        }
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
}

export const dashboardController = new DashboardController();
