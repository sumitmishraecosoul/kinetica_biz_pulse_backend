import { logger } from '@/utils/logger';

export class CacheService {
  private cache: Map<string, { value: any; expiry: number }> = new Map();
  private isConnected: boolean = true; // In-memory cache is always "connected"

  constructor() {
    logger.info('Using in-memory cache (Redis not available)');
  }

  /**
   * Set a key-value pair in cache
   */
  async set(key: string, value: any, ttlSeconds: number = 3600): Promise<void> {
    try {
      const expiry = Date.now() + (ttlSeconds * 1000);
      this.cache.set(key, { value, expiry });
      logger.debug(`Cache set: ${key} (TTL: ${ttlSeconds}s)`);
    } catch (error) {
      logger.error('Error setting cache:', error);
    }
  }

  /**
   * Get a value from cache
   */
  async get<T>(key: string): Promise<T | null> {
    try {
      const item = this.cache.get(key);
      if (item && item.expiry > Date.now()) {
        logger.debug(`Cache hit: ${key}`);
        return item.value as T;
      }
      
      // Remove expired item
      if (item && item.expiry <= Date.now()) {
        this.cache.delete(key);
      }
      
      logger.debug(`Cache miss: ${key}`);
      return null;
    } catch (error) {
      logger.error('Error getting from cache:', error);
      return null;
    }
  }

  /**
   * Delete a key from cache
   */
  async delete(key: string): Promise<void> {
    try {
      this.cache.delete(key);
      logger.debug(`Cache deleted: ${key}`);
    } catch (error) {
      logger.error('Error deleting from cache:', error);
    }
  }

  /**
   * Clear all cache
   */
  async clear(): Promise<void> {
    try {
      this.cache.clear();
      logger.info('Cache cleared');
    } catch (error) {
      logger.error('Error clearing cache:', error);
    }
  }

  /**
   * Get cache statistics
   */
  async getStats(): Promise<{
    connected: boolean;
    keys: number;
    memory: string;
  }> {
    try {
      // Clean up expired items first
      const now = Date.now();
      for (const [key, item] of this.cache.entries()) {
        if (item.expiry <= now) {
          this.cache.delete(key);
        }
      }

      return {
        connected: true,
        keys: this.cache.size,
        memory: '0' // In-memory cache doesn't track memory usage
      };
    } catch (error) {
      logger.error('Error getting cache stats:', error);
      return {
        connected: false,
        keys: 0,
        memory: '0'
      };
    }
  }

  /**
   * Set multiple key-value pairs
   */
  async mset(keyValuePairs: Record<string, any>, ttlSeconds: number = 3600): Promise<void> {
    try {
      const expiry = Date.now() + (ttlSeconds * 1000);
      
      for (const [key, value] of Object.entries(keyValuePairs)) {
        this.cache.set(key, { value, expiry });
      }
      
      logger.debug(`Cache mset: ${Object.keys(keyValuePairs).length} keys`);
    } catch (error) {
      logger.error('Error setting multiple cache values:', error);
    }
  }

  /**
   * Get multiple values from cache
   */
  async mget<T>(keys: string[]): Promise<(T | null)[]> {
    try {
      const now = Date.now();
      return keys.map(key => {
        const item = this.cache.get(key);
        if (item && item.expiry > now) {
          return item.value as T;
        }
        
        // Remove expired item
        if (item && item.expiry <= now) {
          this.cache.delete(key);
        }
        
        return null;
      });
    } catch (error) {
      logger.error('Error getting multiple cache values:', error);
      return keys.map(() => null);
    }
  }

  /**
   * Check if a key exists
   */
  async exists(key: string): Promise<boolean> {
    try {
      const item = this.cache.get(key);
      if (item && item.expiry > Date.now()) {
        return true;
      }
      
      // Remove expired item
      if (item && item.expiry <= Date.now()) {
        this.cache.delete(key);
      }
      
      return false;
    } catch (error) {
      logger.error('Error checking cache key existence:', error);
      return false;
    }
  }

  /**
   * Get TTL for a key
   */
  async getTTL(key: string): Promise<number> {
    try {
      const item = this.cache.get(key);
      if (item && item.expiry > Date.now()) {
        return Math.ceil((item.expiry - Date.now()) / 1000);
      }
      
      // Remove expired item
      if (item && item.expiry <= Date.now()) {
        this.cache.delete(key);
      }
      
      return -1;
    } catch (error) {
      logger.error('Error getting cache TTL:', error);
      return -1;
    }
  }

  /**
   * Disconnect from cache (no-op for in-memory cache)
   */
  async disconnect(): Promise<void> {
    // In-memory cache doesn't need to disconnect
    this.isConnected = false;
  }
}

export const cacheService = new CacheService();
