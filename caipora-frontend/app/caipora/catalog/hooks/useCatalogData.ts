import { useState, useEffect } from 'react';

export function useCatalogData() {
  const [properties, setProperties] = useState<any[]>([]);
  const [totalCount, setTotalCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(1);
  const itemsPerPage = 12;

  const [options, setOptions] = useState({ 
    biomes: [], ufs: [], cities: [], producer_types: [], confidences: [], car_status: []
  });

  const [filters, setFilters] = useState({
    status: '', biome: '', uf: '', city: '', 
    producerType: '', confidence: '', carStatus: '', identity: ''
  });

  const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
  const apiKey = process.env.NEXT_PUBLIC_API_KEY;

  const updateFilter = (key: string, value: string) => {
    setFilters(prev => ({ ...prev, [key]: value }));
    setPage(1);
  };

  const clearFilters = () => {
    setFilters({
      status: '', biome: '', uf: '', city: '', 
      producerType: '', confidence: '', carStatus: '', identity: ''
    });
    setPage(1);
  };

  // Sincronização de Opções
  useEffect(() => {
    const updateOptions = async () => {
      const query: any = {
        status: filters.status, biome: filters.biome, uf: filters.uf,
        city: filters.city, producer_type: filters.producerType,
        confidence: filters.confidence, car_status: filters.carStatus,
        property_identity_type: filters.identity
      };
      Object.keys(query).forEach(key => !query[key] && delete query[key]);
      const params = new URLSearchParams(query);

      try {
        const res = await fetch(`${apiUrl}/compliance/filter-options?${params}`, {
          headers: { "X-API-Key": apiKey || "" }
        });
        const data = await res.json();
        setOptions(data);
      } catch (e) { console.error("Erro na sincronia de filtros", e); }
    };
    updateOptions();
  }, [filters, apiUrl, apiKey]);

  // Busca da Lista
  useEffect(() => {
    const fetchList = async () => {
      setLoading(true);
      const query: any = {
        limit: itemsPerPage.toString(),
        offset: ((page - 1) * itemsPerPage).toString(),
        status: filters.status, biome: filters.biome, uf: filters.uf,
        city: filters.city, producer_type: filters.producerType,
        confidence: filters.confidence, car_status: filters.carStatus,
        property_identity_type: filters.identity
      };
      Object.keys(query).forEach(key => !query[key] && delete query[key]);
      const params = new URLSearchParams(query);

      try {
        const response = await fetch(`${apiUrl}/compliance/list?${params}`, {
          headers: { "X-API-Key": apiKey || "", "Content-Type": "application/json" }
        });
        const result = await response.json();
        setProperties(result.items || []);
        setTotalCount(result.total || 0);
      } catch (error) { 
        console.error("Erro ao buscar lista:", error);
        setProperties([]); 
      } finally { setLoading(false); }
    };
    fetchList();
  }, [filters, page, apiUrl, apiKey]);

  return {
    properties, totalCount, loading, page, setPage,
    options, filters, updateFilter, clearFilters,
    itemsPerPage, totalPages: Math.ceil(totalCount / itemsPerPage) || 1
  };
}