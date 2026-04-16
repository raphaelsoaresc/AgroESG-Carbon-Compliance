import { useState, useEffect, useCallback, useRef } from 'react';
import { supabase } from '../lib/supabase';
import { AuditData, DEMO_IDS } from '../types';
import {
  getAnalysisReason,
  formattedDate,
  determineStatusColor,
  translateConfidence
} from '../lib/audit-utils';
import { getCenterPoint, formatCurrency } from '../utils';

export function useAudit() {
  const [session, setSession] = useState<any>(null);
  const [isAdmin, setIsAdmin] = useState(false);
  const [carId, setCarId] = useState('');
  const [data, setData] = useState<AuditData | null>(null);
  const [loading, setLoading] = useState(false);
  const [searchCount, setSearchCount] = useState(0);
  const [showPayment, setShowPayment] = useState(false);
  const [preferenceId, setPreferenceId] = useState<string | null>(null);
  const [limitReached, setLimitReached] = useState(false);

  // REF para controlar o contador sem disparar a recriação da função performSearch
  const searchCountRef = useRef(0);

  useEffect(() => {
    supabase.auth.getSession().then(({ data: { session } }) => {
      setSession(session);
      setIsAdmin(!!session);
    });
    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event, session) => {
      setSession(session);
      setIsAdmin(!!session);
    });
    return () => subscription.unsubscribe();
  }, []);

  useEffect(() => {
    const savedCount = localStorage.getItem('caipora_search_count');
    if (savedCount && !isAdmin) {
      const count = parseInt(savedCount);
      setSearchCount(count);
      searchCountRef.current = count; // Sincroniza a REF
      if (count >= 3) setLimitReached(true);
    }
  }, [isAdmin]);

  const handleUnlockReport = useCallback(async () => {
    setLoading(true);
    try {
      const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "").replace(/\/$/, "");
      const apiKey = process.env.NEXT_PUBLIC_API_KEY;
      const response = await fetch(`${apiUrl}/payments/create-preference`, {
        method: "POST",
        headers: { "Content-Type": "application/json", "X-API-Key": apiKey || "" },
        body: JSON.stringify({ car_id: carId, email: "compliance@agrimarketintel.com" })
      });
      const result = await response.json();
      let prefId = result.id || result.preference_id;
      if (prefId) {
        setPreferenceId(prefId);
        setShowPayment(true);
      }
    } catch (error) {
      console.error("Erro ao criar preferência de pagamento:", error);
    } finally {
      setLoading(false);
    }
  }, [carId]);

  const performSearch = useCallback(async (idToSearch: string) => {
    if (!idToSearch) return;

    const { data: { session: freshSession } } = await supabase.auth.getSession();
    const isDemo = DEMO_IDS.includes(idToSearch);

    // Usa a REF para checar o limite, evitando dependência direta do estado searchCount
    if (!isDemo && !freshSession && searchCountRef.current >= 3) {
      setLimitReached(true);
      setCarId(idToSearch);
      setData(null);
      return;
    }

    setLoading(true);
    setCarId(idToSearch);
    setShowPayment(false);
    setPreferenceId(null);
    setLimitReached(false);

    const apiUrl = (process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
    const apiKey = process.env.NEXT_PUBLIC_API_KEY;

    const headers: any = {
      "X-API-Key": apiKey || "",
      "Content-Type": "application/json"
    };

    if (freshSession?.access_token) {
      headers["Authorization"] = `Bearer ${freshSession.access_token}`;
    }

    try {
      const response = await fetch(`${apiUrl}/compliance/car/${idToSearch}`, { headers });
      if (!response.ok) throw new Error("Falha na requisição");
      
      const record = await response.json();
      
      const parseGeom = (g: any) => {
        if (!g) return null;
        return typeof g === 'string' ? JSON.parse(g) : g;
      };

      const mainGeometry = parseGeom(record.geom_car_total || record.geometry);

      if (!isDemo && !freshSession) {
        const newCount = searchCountRef.current + 1;
        searchCountRef.current = newCount; // Atualiza a REF imediatamente
        setSearchCount(newCount); // Atualiza o estado para a UI
        localStorage.setItem('caipora_search_count', newCount.toString());
        if (newCount >= 3) setLimitReached(true);
      }

      // MAPEAMENTO SINCRONIZADO COM SCHEMAS.PY E TYPES.TS
      setData({
        // 1. Identificação Básica
        propertyId: record.property_id,
        propertyAlias: record.property_alias,
        property_identity_type: record.property_identity_type,
        area_ha: record.area_ha,
        area_geometria_ha: record.area_geometria_ha,
        property_area_ha: record.property_area_ha,
        area_liquida_ha: record.area_liquida_ha,
        fiscal_modules: record.fiscal_modules,
        city: record.city,
        uf_origem: record.uf_origem,
        car_status: record.car_status,
        // Nova Coluna: Auditoria de Status
        car_status_spatial: record.car_status_spatial,

        // 2. Localização e Perícia
        latitude: record.latitude,
        longitude: record.longitude,
        geometry: mainGeometry,
        // Nova Coluna: Centroide
        centroid: parseGeom(record.centroid),
        car_bbox: record.car_bbox,
        critical_contact_point: record.critical_contact_point,
        max_slope_degrees: record.max_slope_degrees,
        relief_classification: record.relief_classification,

        // 3. Identidades e Categorias
        is_settlement_identity: record.is_settlement_identity,
        is_traditional_identity: record.is_traditional_identity,
        is_quilombo_identity: record.is_quilombo_identity,
        is_ti_identity: record.is_ti_identity,
        is_uc_identity: record.is_uc_identity,
        producer_size_category: record.producer_size_category,
        is_small_holder: record.is_small_holder,

        // 4. Status e Confiança
        final_eligibility_status: record.final_eligibility_status,
        final_eligibility_status_detailed: record.final_eligibility_status_detailed,
        is_technically_blocked: record.is_technically_blocked,
        is_missing_geometry: record.is_missing_geometry,
        geospatial_confidence_level: record.geospatial_confidence_level,
        data_reliability_index: record.data_reliability_index,
        data_source_quality: record.data_source_quality,
        forensic_summary: record.forensic_summary,
        analyzed_at: record.analyzed_at,
        processed_at: record.processed_at,

        // 5. Objetos Complexos (Mapeamento Direto)
        financial_liabilities: record.financial_liabilities,
        environmental_score: record.environmental_score,
        deforestation_metrics: record.deforestation_metrics,
        social_score: record.social_score,
        risk_analysis: record.risk_analysis,

        // 6. Geometrias Adicionais
        geom_car_total: mainGeometry,
        geom_embargos: parseGeom(record.geom_embargos),
        geom_desmatamento: parseGeom(record.geom_desmatamento),
        geom_eudr: parseGeom(record.geom_eudr),
        geom_areas_protegidas: parseGeom(record.geom_areas_protegidas),
        geom_assentamentos: parseGeom(record.geom_assentamentos),
        geom_conflito_app: parseGeom(record.geom_conflito_app),
        geom_adjacencia_risco: parseGeom(record.geom_adjacencia_risco),

        // --- UI HELPERS ---
        carNumber: record.property_id,
        status: record.final_eligibility_status,
        color: determineStatusColor(record.final_eligibility_status, record.geospatial_confidence_level),
        metrics: `Bioma: ${record.environmental_score?.biome_name || 'N/A'} | Confiança: ${translateConfidence(record.geospatial_confidence_level)}`,
        isCensored: !!freshSession ? false : !isDemo,
        mapCenterCoords: [record.latitude, record.longitude],
        
        liabilityTotal: formatCurrency(record.financial_liabilities?.estimated_financial_liability_brl || 0),
        liabilityAmbientalTotal: formatCurrency(
          (record.financial_liabilities?.liability_deforestation_brl || 0) + 
          (record.financial_liabilities?.liability_rl_brl || 0) +
          (record.financial_liabilities?.liability_app_brl || 0)
        ),

        evidenceList: [
          ...(record.risk_analysis?.evidence_admin ? record.risk_analysis.evidence_admin.split(' | ') : []),
          ...(record.risk_analysis?.evidence_social ? record.risk_analysis.evidence_social.split(' | ') : []),
          ...(record.risk_analysis?.evidence_environmental ? record.risk_analysis.evidence_environmental.split(' | ') : []),
          ...(record.risk_analysis?.evidence_infrastructure ? record.risk_analysis.evidence_infrastructure.split(' | ') : []),
        ],
      });
    } catch (error) {
      console.error("Erro na busca de compliance:", error);
    } finally {
      setLoading(false);
    }
  }, []); // Dependência VAZIA: A função nunca muda, matando o loop infinito.

  const logout = useCallback(async () => {
    await supabase.auth.signOut();
    window.location.reload();
  }, []);

  return { 
    carId, 
    setCarId, 
    data, 
    loading, 
    searchCount, 
    showPayment, 
    preferenceId, 
    limitReached, 
    handleUnlockReport, 
    performSearch, 
    isAdmin, 
    logout 
  };
}