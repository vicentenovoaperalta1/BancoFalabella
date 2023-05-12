
CREATE OR REPLACE TABLE `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.trf_seguimiento_cartera_cmr_antigua` AS  --FALTA AUTOMATIZAR FECHA

WITH 
    MAESTRA_ID AS (SELECT CAST(ID_CLIENTE AS INT64) AS IdCliente, ID_CORPORATIVO AS IdCorporativo
                   FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__hml_bfa_cl_datalake_prd.svw_hml_maestra_cliente_id_corp`
                   ),

    FRAUDE AS (SELECT IdCliente, 
                      MAX(FechaFraude) AS FechaFraude, 
                      1                AS MarcaFraude
               FROM (SELECT RUT AS IdCliente, MAX(fecha_fraude) AS FechaFraude
                     FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.BZ_RUTERO_FRAUDES`
                     GROUP BY 1

                     UNION ALL 

                     SELECT ID_CLIENTE AS IdCliente, MAX(FECHA_VENTA) AS FechaFraude
                     FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.Fraudes_Base`
                     GROUP BY 1

                     UNION ALL 

                     SELECT IdCliente, MAX(FechaFraude) FechaFraude
                     FROM (SELECT *, CAST(X.FECHA_INFORMACION AS DATE) AS  FechaFraude,
                           FROM `fif-bfa-cl-risk-discovery.svw_fif_bfa_cl_dmr_risk_prod__trf_bfa_cl_datamart_riesgo_mir_prd.svw_TRANSACCION_FINANCIERA` X
                           LEFT JOIN `fif-bfa-cl-risk-discovery.svw_fif_bfa_cl_dmr_risk_prod__trf_bfa_cl_datamart_riesgo_mir_prd.svw_FRAUDE_TRANSACCION` Y ON X.ID_TRX = Y.ID_TRX
                           LEFT JOIN  MAESTRA_ID Z ON X.COD_CONTRAPARTE = Z.IdCorporativo
                           WHERE Y.ID_TRX is not null
                           )
                           GROUP BY 1
                    ) 
               GROUP BY 1
               ),

    FRAUDE2 AS (SELECT CONCAT("00",CONTRATO_FINAL) AS Contrato, 1 AS MarcaFraude2
               FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.VN_Fraudes_Hasta_20220930`
               ),
		
    MAESTRA AS (SELECT *, LAG(IdSitu) OVER (PARTITION BY Contrato ORDER BY FechaFoto ASC)          AS IdSitu_Mmenos1
      
                FROM (SELECT FECHA_FOTO                                                                 AS FechaFoto,
                             CONTRATO                                                                   AS Contrato,
                             ""                                                                         AS ContratoSAT,
                             ID_CLIENTE                                                                 AS IdCliente,
                             CENTALTA                                                                   AS SucursalApertura,
                             F_CUPO                                                                     AS CupoCMR,
                             COACTI,
                             SAFE_CAST(ID_DIA_APER AS DATE)                                             AS FechaApertura,
                             SAFE_CAST(ID_DIA_NAC AS DATE)                                              AS FechaNacimiento,
                             F_SALDEUD                                                                  AS DeudaL1,
                             F_SALDE_SU                                                                 AS DeudaL2,
                             F_AVA_OCU,                                           
                             F_SALDO                                                                    AS DeudaCMR,
                             MORA_AC                                                                    AS DiasMora,
                             ID_SITU                                                                    AS IdSitu,
                             SAFE_CAST(ID_DIA_RENEG AS DATE)                                            AS FechaRenegociacion,
                             SAFE_CAST(ID_DIA_MAXULTMOV AS DATE)                                        AS FechaUltimoMovimiento,
                             CAST(Id_Dia_Cast AS DATE)                                                  AS IdDiaCast,

                      FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__mgr_bfa_cl_datalake_prd.svw_mgr_dwh_dbmark_agg_rsg_maestra_mes_hist` 
                      WHERE FECHA_FOTO >= '2021-01-01' AND FECHA_FOTO <= '2022-04-01'

                      UNION DISTINCT

                      SELECT DATE_TRUNC(FechaCarga, MONTH)                                              AS FechaFoto,
                             Contrato,    
                             ContratoSAT,                            
                             IdCliente,                               
                             Centalta                                                                   AS SucursalApertura,
                             FCupo                                                                      AS CupoCMR,
                             Coacti,                                
                             IdDiaAper                                                                  AS FechaApertura,
                             IdDiaNac                                                                   AS FechaNacimiento,
                             FSALDEUD                                                                   AS DeudaL1,
                             FSaldeSu                                                                   AS DeudaL2,
                             FAVAOCU,                                                               
                             FSALDO                                                                     AS DeudaCMR,
                             MoraAc                                                                     AS DiasMora,
                             IdSitu                                                                     AS IdSitu,
                             IdDiaReneg                                                                 AS FechaRenegociacion,
                             IdDiaMaxUltMov                                                             AS FechaUltimoMovimiento,
                             IdDiaCast,

                      FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_contrato_maestra_riesgo_fcon` 
                      WHERE FechaCarga IN('2022-05-31','2022-06-30','2022-07-31','2022-08-31','2022-09-30','2022-10-31','2022-11-30','2022-12-31','2023-01-31','2023-02-28','2023-03-31','2023-04-30') --FALTA AUTOMATIZAR FECHA
                            AND (DATE_TRUNC(IdDiaAper, MONTH) <> '2022-09-01' OR IdDiaNac <> '1990-05-05' OR FCupo<>50000) --DE PRUEBA
                      )
                ),

    BLOQUEOS AS (SELECT *, DATE_TRUNC(FechaCarga, MONTH) AS FechaFoto
                 FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_risk_prd__trf_bfa_cl_risk_portfolio_prd.svw_trf_contrato_activo_stock_bloqueo`
                 WHERE DescripcionBloqueo NOT IN("Temporal","Fraude") AND FechaCarga >= "2022-01-01" AND FechaCarga = LAST_DAY(FechaCarga, MONTH)
                 ),
    
    STOCK AS (SELECT AA.FechaFoto, 
                     AA.IdCliente,
                     AA.Contrato,
                     AA.ContratoSAT,
                     SucursalApertura,
                     CupoCMR,
                     DeudaCMR,
                     DeudaL1,
                     DeudaL2,
                     IdSitu,
                     DiasMora,
                     FechaApertura,
                     FechaNacimiento,
                     DATE_DIFF(AA.FechaFoto, FechaNacimiento, MONTH)  AS EdadMeses,
                     FechaRenegociacion,
                     FechaUltimoMovimiento,
                     COACTI                                                               AS CodigoActividad,
                     DescripcionBloqueo,
                     CASE WHEN IFNULL(SAFE_CAST(COACTI AS INT64), 0) IN (1, 3, 5, 8, 15)             THEN 'Independiente'
                          WHEN IFNULL(SAFE_CAST(COACTI AS INT64), 0) IN (2, 4, 6, 7, 9, 10, 14, 16)  THEN 'Dependiente'
                          WHEN IFNULL(SAFE_CAST(COACTI AS INT64), 0) = 11                            THEN 'Jubilado'
                          WHEN IFNULL(SAFE_CAST(COACTI AS INT64), 0) = 12                            THEN 'Dueña de Casa'
                          WHEN IFNULL(SAFE_CAST(COACTI AS INT64), 0) = 13                            THEN 'Estudiante'
                     ELSE 'Otros' END                                                     AS TipoEmpleo,
                     CASE WHEN DiasMora > 0 THEN 1 ELSE 0 END                             AS TieneMora,
                     CASE WHEN GREATEST(DeudaL2, 0) > 0 THEN 'Super Avance'
                          WHEN GREATEST(AA.F_AVA_OCU , 0) > 0 THEN 'Avance'
                          WHEN GREATEST(DeudaL1 , 0) > 0 THEN 'TC'
                     ELSE 'Sin deuda' END                                                 AS SegmentoUso,
                     CASE WHEN SucursalApertura = '9999' THEN 1 ELSE 0 END                AS AperturaWeb,
                     BB.FechaFraude,
                     IFNULL(BB.MarcaFraude, 0)                                            AS MarcaFraude,
                     CASE WHEN DATE_TRUNC(FechaApertura, MONTH) = AA.FechaFoto THEN 1 ELSE 0 END AS Apertura,
                     CASE WHEN AA.FechaFoto < '2022-08-01' AND IdSitu=1 AND IdSitu_Mmenos1=2    THEN "Reapertura"
                          WHEN IdSitu=1 AND IdSitu_Mmenos1=2 AND DescripcionBloqueo IS NOT NULL THEN "Reapertura"
                          WHEN IdSitu=1 AND IdSitu_Mmenos1=2                                    THEN "Desbloqueo"
                          ELSE "Originacion" END                   AS Reapertura,
                     
              FROM MAESTRA AA
              LEFT JOIN FRAUDE       BB ON AA.IdCliente = BB.IdCliente
              LEFT JOIN BLOQUEOS     CC ON AA.ContratoSAT = CC.ContratoSAT AND AA.FechaFoto = DATE_ADD(CC.FechaFoto, INTERVAL 1 MONTH)
              WHERE AA.FechaFoto >= '2021-01-01' AND (DATE_TRUNC(FechaApertura, MONTH) = AA.FechaFoto OR (IdSitu=1 AND IdSitu_Mmenos1=2))
              ),

    MARCA_RENE AS (SELECT Contrato, 1 AS Renegociacion
                   FROM (SELECT FechaOperacion, AA.Contrato, Tipofac, DesTipfac, ROW_NUMBER() OVER (PARTITION BY AA.Contrato ORDER BY FechaOperacion ASC) AS Ranking

                         FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_evento_finan_transacc_cmr` AA

                         LEFT JOIN STOCK BB ON AA.Contrato = BB.ContratoSAT
                         WHERE FechaOperacion > '2021-01-01' AND BB.ContratoSAT IS NOT NULL AND AA.FechaOperacion <= DATE_ADD(BB.FechaApertura, INTERVAL 1 MONTH) 
                         ) 
                   WHERE Ranking <= 10 AND Tipofac = 96022 /* CARGO RENE */
                   GROUP BY 1
                   ),

    LOGICA_RENTA AS (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 2 MONTH)           AS FechaFoto,
                            ID_CLIENTE                                       AS IdCliente,
                            RENTA_FINAL                                      AS RentaEstimada,
                            TIPO_RENTA_FINAL                                 AS TipoRentaEstimada,
                            RENTA_COMPRAS                                    AS RentaCompras,
                            CASE WHEN RENTA_COMPRAS > 0 THEN 1 ELSE 0 END    AS TenenciaRetail
                     FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_dev.trf_logica_renta`
                     WHERE FECHA_FOTO >= '2020-12-01'
                     ),

    R04 AS (SELECT DATE_ADD(AA.FechaFoto, INTERVAL 2 MONTH) AS FechaFoto,
                   AA.* EXCEPT (FechaFoto),
                   SUM(MarcaBancarizado)                    AS RecuentoBancarizadoU06M,
            FROM (SELECT FECHA_FOTO                             AS FechaFoto,
                         ID_CLIENTE                             AS IdCliente,
                         1000 * LINEA_DISP                      AS LineaDisponible,
                         1000 * DCONSUMO                        AS DeudaConsumo,
                         1000 * DCOMERCIAL                      AS DeudaComercial,
                         1000 * DHIPOTECAR                      AS DeudaHipotecario,
                         SUM(LineaDisponible)                   AS LineaDisponibleSumU03M,
                         SUM(DeudaConsumo)                      AS DeudaConsumoSumU03M,
                         SUM(DeudaComercial)                    AS DeudaComercialSumU03M,
                         SUM(DeudaHipotecario)                  AS DeudaHipotecarioSumU03M,
                         AVG(LineaDisponible)                   AS LineaDisponiblePromU03M,
                         AVG(DeudaConsumo)                      AS DeudaConsumoPromU03M,
                         AVG(DeudaComercial)                    AS DeudaComercialPromU03M,
                         AVG(DeudaHipotecario)                  AS DeudaHipotecarioPromU03M,
                         SUM(MesesConR04)                       AS MesesConR04
                         
                  FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_btd_vw_ptt_r04` AA
                  LEFT JOIN (SELECT FECHA_FOTO         AS FechaFoto,
                                    ID_CLIENTE         AS IdCliente,
                                    1000 * LINEA_DISP  AS LineaDisponible,
                                    1000 * DCONSUMO    AS DeudaConsumo,
                                    1000 * DCOMERCIAL  AS DeudaComercial,
                                    1000 * DHIPOTECAR  AS DeudaHipotecario,
                                    1                  AS MesesConR04
                                    
                             FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_btd_vw_ptt_r04`
                             WHERE FECHA_FOTO >= '2020-06-01'
                             ) BB ON AA.ID_CLIENTE = BB.IdCliente AND BB.FechaFoto <= AA.FECHA_FOTO AND BB.FechaFoto >= DATE_SUB(AA.FECHA_FOTO, INTERVAL 2 MONTH)
                  WHERE AA.FECHA_FOTO >= '2020-11-01'
                  GROUP BY 1, 2, 3, 4, 5, 6
                  ) AA
            LEFT JOIN (SELECT FECHA_FOTO         AS FechaFoto,
                              ID_CLIENTE         AS IdCliente,
                              1000 * LINEA_DISP  AS LineaDisponibleU06M,
                              1000 * DCONSUMO    AS DeudaConsumoU06M,
                              1000 * DCOMERCIAL  AS DeudaComercialU06M,
                              1000 * DHIPOTECAR  AS DeudaHipotecarioU06M,
                              1                  AS MesesConR04U06M,
                              CASE WHEN LINEA_DISP + DCONSUMO + DCOMERCIAL + DHIPOTECAR > 0 THEN 1 ELSE 0 END AS MarcaBancarizado
                              
                       FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_btd_vw_ptt_r04`
                       WHERE FECHA_FOTO >= '2020-06-01'
                       ) CC ON AA.IdCliente = CC.IdCliente AND CC.FechaFoto <= AA.FechaFoto AND CC.FechaFoto >= DATE_SUB(AA.FechaFoto, INTERVAL 5 MONTH)
            
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15
            ),

    VINTAGE AS (SELECT AA.FechaFoto, 
                       AA.IdCliente, 
                       AA.Contrato,
                
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 2 MONTH) = AB.FechaFoto THEN GREATEST(IFNULL(AB.DeudaTotal, 0), 0) ELSE 0 END)  AS DeudaTotalMes02,
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 2 MONTH) = AB.FechaFoto THEN GREATEST(IFNULL(AB.DeudaL1, 0), 0)    ELSE 0 END)  AS DeudaL1Mes02,
                       COUNT(DISTINCT AB.FechaFoto)                                                                                                  AS MesesConVintageMes02,
                       MAX(CASE WHEN IFNULL(AB.DeudaL2, 0) > 0 THEN 1 ELSE 0 END)                                                                    AS MarcaDeudaSAMes02,
                       MAX(IFNULL(AB.MarcaMora30, 0))                                                                                                AS MarcaMora30Mes02,
                       MAX(CASE WHEN IFNULL(AB.FechaUltimoMovimiento,'1800-01-01') > '1800-01-01' OR AB.DeudaTotal >=10000 THEN 1 ELSE 0 END)        AS RiesgoVivoMes02,
                 
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 3 MONTH) = BB.FechaFoto THEN GREATEST(IFNULL(BB.DeudaTotal, 0), 0) ELSE 0 END)  AS DeudaTotalMes03,
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 3 MONTH) = BB.FechaFoto THEN GREATEST(IFNULL(BB.DeudaL1, 0), 0)    ELSE 0 END)  AS DeudaL1Mes03,
                       COUNT(DISTINCT BB.FechaFoto)                                                                                                  AS MesesConVintageMes03,
                       MAX(CASE WHEN IFNULL(BB.DeudaL2, 0) > 0 THEN 1 ELSE 0 END)                                                                    AS MarcaDeudaSAMes03,
                       MAX(IFNULL(BB.MarcaMora30, 0))                                                                                                AS MarcaMora30Mes03,
                       MAX(CASE WHEN IFNULL(BB.FechaUltimoMovimiento,'1800-01-01') > '1800-01-01' OR BB.DeudaTotal >=10000 THEN 1 ELSE 0 END)        AS RiesgoVivoMes03,
                 
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 6 MONTH) = CC.FechaFoto THEN GREATEST(IFNULL(CC.DeudaTotal, 0), 0) ELSE 0 END)  AS DeudaTotalMes06,
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 6 MONTH) = CC.FechaFoto THEN GREATEST(IFNULL(CC.DeudaL1, 0), 0)    ELSE 0 END)  AS DeudaL1Mes06,
                       COUNT(DISTINCT CC.FechaFoto)                                                                                                  AS MesesConVintageMes06,
                       MAX(CASE WHEN IFNULL(CC.DeudaL2, 0) > 0 THEN 1 ELSE 0 END)                                                                    AS MarcaDeudaSAMes06,
                       MAX(IFNULL(CC.MarcaMora30, 0))                                                                                                AS MarcaMora30Mes06,
                       MAX(IFNULL(CC.MarcaMora60, 0))                                                                                                AS MarcaMora60Mes06,
                       MAX(CASE WHEN IFNULL(CC.FechaUltimoMovimiento,'1800-01-01') > '1800-01-01' OR CC.DeudaTotal >=10000 THEN 1 ELSE 0 END)        AS RiesgoVivoMes06,

                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 12 MONTH) = DD.FechaFoto THEN GREATEST(IFNULL(DD.DeudaTotal, 0), 0) ELSE 0 END) AS DeudaTotalMes12,
                       MAX(CASE WHEN DATE_ADD(AA.FechaFoto, INTERVAL 12 MONTH) = DD.FechaFoto THEN GREATEST(IFNULL(DD.DeudaL1, 0), 0)    ELSE 0 END) AS DeudaL1Mes12,
                       COUNT(DISTINCT DD.FechaFoto)                                                                                                  AS MesesConVintageMes12,
                       MAX(CASE WHEN IFNULL(DD.DeudaL2, 0) > 0 THEN 1 ELSE 0 END)                                                                    AS MarcaDeudaSAMes12,
                       MAX(IFNULL(DD.MarcaMora30, 0))                                                                                                AS MarcaMora30Mes12,
                       MAX(IFNULL(DD.MarcaMora60, 0))                                                                                                AS MarcaMora60Mes12,
                       MAX(IFNULL(DD.MarcaMora90, 0))                                                                                                AS MarcaMora90Mes12,
                       MAX(CASE WHEN IFNULL(DD.FechaUltimoMovimiento,'1800-01-01') > '1800-01-01' OR DD.DeudaTotal >=10000 THEN 1 ELSE 0 END)        AS RiesgoVivoMes12,

                FROM STOCK AA
                LEFT JOIN (SELECT FechaFoto, 
                                  Contrato, 
                                  DeudaL1, 
                                  DeudaL2, 
                                  DeudaCMR                                   AS DeudaTotal, 
                                  CASE WHEN DiasMora >= 30 THEN 1 ELSE 0 END AS MarcaMora30,
                                  1                                          AS MesesConVintage,
                                  FechaUltimoMovimiento,

                           FROM MAESTRA
                           WHERE FechaFoto >= '2021-01-01') AB ON AA.Contrato = AB.Contrato 
                                                               AND AB.FechaFoto >= AA.FechaFoto 
                                                               AND AB.FechaFoto <= DATE_ADD(AA.FechaFoto, INTERVAL 2 MONTH)
                LEFT JOIN (SELECT FechaFoto, 
                                  Contrato, 
                                  DeudaL1, 
                                  DeudaL2, 
                                  DeudaCMR                                   AS DeudaTotal, 
                                  CASE WHEN DiasMora >= 30 THEN 1 ELSE 0 END AS MarcaMora30,
                                  1                                          AS MesesConVintage,
                                  FechaUltimoMovimiento,
                                  
                           FROM MAESTRA
                           WHERE FechaFoto >= '2021-01-01') BB ON AA.Contrato = BB.Contrato 
                                                               AND BB.FechaFoto >= AA.FechaFoto 
                                                               AND BB.FechaFoto <= DATE_ADD(AA.FechaFoto, INTERVAL 3 MONTH)
                LEFT JOIN (SELECT FechaFoto, 
                                  Contrato, 
                                  DeudaL1, 
                                  DeudaL2, 
                                  DeudaCMR                                   AS DeudaTotal, 
                                  CASE WHEN DiasMora >= 30 THEN 1 ELSE 0 END AS MarcaMora30,
                                  CASE WHEN DiasMora >= 60 THEN 1 ELSE 0 END AS MarcaMora60,
                                  1                                          AS MesesConVintage,
                                  FechaUltimoMovimiento,
                                  
                           FROM MAESTRA
                           WHERE FechaFoto >= '2021-01-01') CC ON AA.Contrato = CC.Contrato 
                                                               AND CC.FechaFoto >= AA.FechaFoto 
                                                               AND CC.FechaFoto <= DATE_ADD(AA.FechaFoto, INTERVAL 6 MONTH)
                LEFT JOIN (SELECT FechaFoto, 
                                  Contrato, 
                                  DeudaL1, 
                                  DeudaL2, 
                                  DeudaCMR                                   AS DeudaTotal, 
                                  CASE WHEN DiasMora >= 30 THEN 1 ELSE 0 END AS MarcaMora30,
                                  CASE WHEN DiasMora >= 60 THEN 1 ELSE 0 END AS MarcaMora60,
                                  CASE WHEN DiasMora >= 90 THEN 1 ELSE 0 END AS MarcaMora90,
                                  1                                          AS MesesConVintage,
                                  FechaUltimoMovimiento,
                                  
                           FROM MAESTRA
                           WHERE FechaFoto >= '2021-01-01') DD ON AA.Contrato = DD.Contrato 
                                                               AND DD.FechaFoto >= AA.FechaFoto 
                                                               AND DD.FechaFoto <= DATE_ADD(AA.FechaFoto, INTERVAL 12 MONTH)
                GROUP BY 1, 2, 3
                ),

    ML_PREAP AS (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 2 MONTH)   AS FechaFoto,
                        FECHA_FOTO                               AS FechaCortePreaprobados,
                        ID_CLIENTE                               AS IdCliente,
                        SEGMENTO                                 AS ClasificacionPreaprobados,
                        PROB_PREAPROBADOS                        AS ProbPreaprobados,
                        CASE WHEN PROB_PREAPROBADOS <= 0.05 THEN '01. <= 5%'
                             WHEN PROB_PREAPROBADOS <= 0.10 THEN '02. >5 <=10%'
                             WHEN PROB_PREAPROBADOS <= 0.15 THEN '03. >10 <=15%'
                             WHEN PROB_PREAPROBADOS <= 0.20 THEN '04. >15 <=20%'
                             WHEN PROB_PREAPROBADOS <= 0.25 THEN '05. >20 <=25%'
                        ELSE '06. >25%' END AS TramoProbabilidadPreap,
                 
                        CASE WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.0150 THEN '01. AAA'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.0320 THEN '02. AA'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.0550 THEN '03. A'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.0800 THEN '04. B'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.1020 THEN '05. C'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.1542 THEN '06. D'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.1736 THEN '07. E'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.2030 THEN '08. F'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS < 0.2397 THEN '09. G'
                             WHEN FECHA_FOTO < '2022-08-01' AND PROB_PREAPROBADOS <= 1     THEN '10. H'
                             WHEN FECHA_FOTO < '2022-08-01' AND (PROB_PREAPROBADOS > 1 OR PROB_PREAPROBADOS IS NULL) THEN 'Otros'
                 
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0121 THEN '01. AAA'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0221 THEN '02. AA'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0325 THEN '03. A'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0437 THEN '04. B'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0622 THEN '05. C'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0735 THEN '06. D'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.0880 THEN '07. E'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.1010 THEN '08. F'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS < 0.1276 THEN '09. G'
                             WHEN FECHA_FOTO >= '2022-08-01' AND PROB_PREAPROBADOS <= 1     THEN '10. H'
                             WHEN FECHA_FOTO >= '2022-08-01' AND (PROB_PREAPROBADOS > 1 OR PROB_PREAPROBADOS IS NULL)  THEN 'Otros'
                             END AS SegmentoPreaprobados
                 FROM `bfa-cl-preapproved-dev.shr_bfa_cl_preapproved_dev_bfa_cl_risk_dev.vw_evaluacion_mensual_preaprobados`
                 WHERE FECHA_FOTO >= '2020-11-01'
                 ),

    ML_PREAP_NUEVO AS (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 2 MONTH)    AS FechaFoto,
                              ID_CLIENTE                                AS IdCliente,
                              PROB                                      AS PROB_REEST,
                              CASE WHEN PROB < 0.0121 THEN '01. AAA'
                                   WHEN PROB < 0.0221 THEN '02. AA'
                                   WHEN PROB < 0.0325 THEN '03. A'
                                   WHEN PROB < 0.0437 THEN '04. B'
                                   WHEN PROB < 0.0622 THEN '05. C'
                                   WHEN PROB < 0.0735 THEN '06. D'
                                   WHEN PROB < 0.0880 THEN '07. E'
                                   WHEN PROB < 0.1010 THEN '08. F'
                                   WHEN PROB < 0.1276 THEN '09. G'
                                   WHEN PROB <= 1     THEN '10. H'
                                   WHEN (PROB > 1 OR PROB IS NULL)  THEN 'Otros'
                                   END AS SegmentoPreaprobados_Reest,

                       FROM `bfa-cl-preapproved-dev.trf_bfa_cl_preapproved_preaprobados_modelamiento_2022_dev.trf_preaprobados_muestra_pred`
                       WHERE FECHA_FOTO >= '2021-06-01'
                       ),

    MINIPOLITICA AS (SELECT DATE_ADD(FechaFoto, INTERVAL 1 MONTH) AS FechaFoto, 
                            IdCliente,
                            CumpleMinipoliticaBanco,
                            CumpleMinipoliticaCMR,
                            CumpleMinipoliticaComercial,
                            CumpleMinipoliticaR04,
                            LEAST(CumpleMinipoliticaBanco, CumpleMinipoliticaCMR, CumpleMinipoliticaComercial, CumpleMinipoliticaR04) AS CumpleMinipolitica
                     FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_dev.trf_minipolitica` 
                     WHERE FechaFoto >= '2020-12-01'
                     ),

    VEHICULOS AS (SELECT * EXCEPT(Ranking)
                  FROM (SELECT A.FechaFoto, A.IdCliente, B.TasacionTotal, ROW_NUMBER() OVER (PARTITION BY A.FechaFoto, A.IdCliente ORDER BY A.FechaFoto DESC)   AS Ranking
                        FROM STOCK A
                        
                        LEFT JOIN (SELECT IdCliente,
                                          FechaFoto,
                                          SUM(TasacionTotal)       AS TasacionTotal,

                                   FROM (SELECT CAST(IDCLIENTE AS INT64)                                                                                                                 AS IdCliente,
                                                DATE_TRUNC(upload_dt, MONTH)                                                                                                             AS FechaFoto,
                                                CAST(CASE WHEN IFNULL(TRIM(vr_appraised_amt),"0") IN("","null") THEN "0" ELSE IFNULL(TRIM(vr_appraised_amt),"0") END AS FLOAT64)         AS TasacionTotal,
                                                vr_type                                                                                                                                  AS TipoAuto,
                                                CAST(CONCAT(CASE WHEN CAST(IFNULL(vr_year,"0") AS INT) < 1900 THEN "1900" ELSE vr_year END,'-01-01') AS DATE)                            AS FechaAuto,

                                         FROM `tc-sc-bi-bigdata-cdl-prod.acc_cor_cl_cdp_third_party_prod.vw_btd_ebim_vehicles` AA

                                         LEFT JOIN `fif-bfa-cl-risk-discovery.svw_bfa_cl_risk_prd__trf_bfa_cl_risk_portfolio_prd.svw_trf_maestra_cliente_riesgo` BB ON AA.num_rut = BB.IdCorporativo
                                         WHERE DATE_TRUNC(upload_dt, MONTH) < '2022-01-01'--------BASES DE 2022 AÜN NO SE CARGAN EN MOTORES
                                         )
                                   
                                   WHERE DATE_DIFF(FechaFoto,FechaAuto,YEAR) <= 15 -----------------antes se consideraba desde 1998 hacia adelante, pero la pauta dice 15 años antiguedad
                                   AND TipoAuto IN ('AUTOMOVIL', 'STATION WAGON', 'CAMIONETA', 'FURGON', 'CAMION', 'MAQUINA INDUSTRIAL', 'JEEP', 'BUS', 'MINIBUS',
                                                                 'TRACTOR', 'TRACTOCAMION', 'AMBULANCIA', 'MAQUINA AGRICOLA', 'CARROBOMBA', 'MICROBUS', 'OMNIBUS', 'TROLEBUS')
                                   GROUP BY 1,2
                                   ) B ON A.IdCliente = B.IdCliente AND A.FechaFoto >= B.FechaFoto AND DATE_DIFF(A.FechaFoto, B.FechaFoto, MONTH) <= 12
                        )
                  WHERE Ranking=1
                  ),

    PROPIEDADES AS (SELECT * EXCEPT(Ranking)
                    FROM (SELECT A.FechaFoto, A.IdCliente, B.AvaluoTotal, ROW_NUMBER() OVER (PARTITION BY A.FechaFoto, A.IdCliente ORDER BY A.FechaFoto DESC)   AS Ranking
                          FROM STOCK A
                          
                          LEFT JOIN (SELECT ID_CLIENTE                       AS IdCliente,
                                            DATE_TRUNC(FECHA_FOTO, MONTH)    AS FechaFoto,
                                            SUM(CAST(AVALUO_TOTAL AS INT64)) AS AvaluoTotal, 
                                     FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__mgr_bfa_cl_datalake_prd.svw_mgr_risk_bienes_raices_hist` 
                                     WHERE FECHA_FOTO >= '2020-06-01' AND FECHA_FOTO < '2022-01-01'--------BASES DE 2022 AÜN NO SE CARGAN EN MOTORES
                                     GROUP BY 1,2

                                     UNION ALL

                                     SELECT CAST(IDCLIENTE AS INT64)                     AS IdCliente,
                                            DATE_TRUNC(upload_dt, MONTH)                 AS FechaFoto,
                                            SUM(CAST(nhr_prop_val_amt AS INT))           AS AvaluoTotal,
                                     FROM `tc-sc-bi-bigdata-cdl-prod.acc_cor_cl_cdp_third_party_prod.vw_btd_ebim_houses` AA

                                     LEFT JOIN `fif-bfa-cl-risk-discovery.svw_bfa_cl_risk_prd__trf_bfa_cl_risk_portfolio_prd.svw_trf_maestra_cliente_riesgo` BB ON AA.nhr_owner_rut_id = BB.IdCorporativo
                                     WHERE DATE_TRUNC(upload_dt, MONTH) < '2022-01-01'--------BASES DE 2022 AÜN NO SE CARGAN EN MOTORES
                                     GROUP BY 1,2
                                     ) B ON A.IdCliente = B.IdCliente AND A.FechaFoto >= B.FechaFoto AND DATE_DIFF(A.FechaFoto, B.FechaFoto, MONTH) <= 12 
                          )
                    WHERE Ranking=1
                    ),

    BASE_PREAPROBADOS AS (SELECT *
                          FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.trf_base_preaprobados_cmr_apertura`
                          ),

    SINACOFI AS (SELECT *
                 FROM (SELECT BB.IdCliente,
                              FechaNacimiento                                                          AS FechaNacimientoSinacofi,
                              CASE WHEN Nacionalidad =  'C' THEN 152 ELSE 999 END                      AS NacionalidadSinacofi, 
                              FechaDefuncion,
                              ROW_NUMBER() OVER (PARTITION BY BB.IdCliente ORDER BY FechaCarga DESC)   AS Ranking
                       FROM `bfa-cl-risk-prd.shr_bfa_cl_risk_prd_bfa_cl_risk_dev.vw_trf_sinacofi_datosresumen` AA
                       LEFT JOIN MAESTRA_ID BB ON AA.IdCliente = BB.IdCorporativo
                       WHERE FechaCarga >= '2021-01-01')
                 WHERE Ranking = 1
                 ),

    MARCA_MOTOR AS (SELECT * EXCEPT(Ranking)
                     FROM(SELECT B.*, Contrato, ROW_NUMBER() OVER (PARTITION BY A.Contrato, A.FechaFoto ORDER BY DRegistro DESC) AS Ranking
                          FROM STOCK A
                          LEFT JOIN (SELECT DATE_TRUNC(CAST(CAST(DRegistro AS DATETIME) AS DATE), MONTH)                                                                                  AS FechaFoto,
                                            CAST(CAST(DRegistro AS DATETIME) AS DATE)                                                                                                     AS FechaSolicitud,
                                            DRegistro,
                                            IdCliente,                                          
                                            NSolic                                                                                                                                        AS NumeroSolicitud, 
                                            NIacpreaprobado,                                         
                                            IFNULL(CAST(AIafpreaprobado AS INT64), 0)                                                                                                     AS MarcaPreaprobadoMotor,
                                            AAevaleafnodename,                                            
                                            ARsltdpolcategoria,                                           
                                            CASE WHEN NIasosolicsucapert = '9999' THEN 'Web' ELSE 'Presencial' END                                                                        AS CanalMotor,
	                           	              AIprevmfirmaautoprev 		                                                                                                                      AS AutorizaPrevired,
	                           	              DIaccocfecrenta 				                                                                                                                      AS FechaRentaSATIF,
	                           	              CASE WHEN CTiposolic = 'EVA' AND IFNULL(CAST(NIaprenta AS INT64), 0) > 0 THEN IFNULL(CAST(NIaprenta AS INT64), 0) ELSE 0 END                  AS RentaSATIF, -- se usa solo si es BO
	                          	              RENTPROMPREV 				                                                                                                                          AS RentaPreviredU03M,
	                          	              CASE WHEN ANTPREVIRED <= 45 AND RENTPROMPREV > 0 THEN RENTPROMPREV ELSE 0 END                                                                 AS RentaPreviredValidaU03M,
	                          	              ANTPREVIRED 					                                                                                                                        AS AntiguedadPrevired,
	                          	              NUMCOTIVAL 					                                                                                                                          AS NumeroCotizacionesU03M,
                                            AExCtaCastigada                                                                                                                               AS MarcaExCastigada,
                                            CAST(NIapavaluopropiedad     AS INT64)                                                                                                        AS AvaluoPropiedad,
                                            CAST(NIaptasacionvehiculo    AS INT64)                                                                                                        AS TasacionVehiculo,
                                            CAST(NIapaniovehiculo        AS INT64)                                                                                                        AS AgnoVehiculo,
                                            IANTECEDENTESDELASOLICITUDAI                                                                                                                  AS ClasificacionApertura,
                                            CASE WHEN ANacionalidad = 'CL' THEN 152 ELSE CAST(ANacionalidad AS INT64) END                                                                 AS NacionalidadMotor,
                                     FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_contrato_tbl_log_da1`
                                     WHERE FechaCarga >= '2020-09-01' AND NOrigen = 2
                                     ) B ON A.IdCliente = B.IdCliente AND A.FechaFoto >= B.FechaFoto
                          )
                     WHERE Ranking = 1 --Me quedo con la última solicitud de cada contrato,fechafoto
                    ),

    INFO_RETAIL AS (SELECT AA.*,
                           IFNULL(BB.ComprasSodimacU12M  , 0) AS ComprasSodimacU12M,
                           IFNULL(CC.ComprasFalabellaU12M, 0) AS ComprasFalabellaU12M,
                           IFNULL(DD.ComprasTottusU12M   , 0) AS ComprasTottusU12M,
                           CASE WHEN IFNULL(BB.ComprasSodimacU12M  , 0) > 0 THEN 1 ELSE 0 END AS TenenciaSodimacU12M,
                           CASE WHEN IFNULL(CC.ComprasFalabellaU12M, 0) > 0 THEN 1 ELSE 0 END AS TenenciaFalabellaU12M,
                           CASE WHEN IFNULL(DD.ComprasTottusU12M   , 0) > 0 THEN 1 ELSE 0 END AS TenenciaTottusU12M
                    FROM (SELECT FechaFoto, 
                                 IdCliente, 
                          FROM STOCK
                          GROUP BY 1,2) AA
                          
                    LEFT  JOIN (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 1 MONTH) AS FechaFoto, ID_CLIENTE AS IdCliente, MONTO_S_12M AS ComprasSodimacU12M
                                FROM `bfa-cl-preapproved-dev.trf_bfa_cl_preapproved_dev.trf_preaprobados_transaccionalidad_sodimac`
                                WHERE FECHA_FOTO >= '2020-12-01') BB ON AA.IdCliente = BB.IdCliente AND AA.FechaFoto = BB.FechaFoto
                    LEFT  JOIN (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 1 MONTH) AS FechaFoto, ID_CLIENTE AS IdCliente, MONTO_F_12M AS ComprasFalabellaU12M
                                FROM `bfa-cl-preapproved-dev.trf_bfa_cl_preapproved_dev.trf_preaprobados_transaccionalidad_falabella`
                                WHERE FECHA_FOTO >= '2020-12-01') CC ON AA.IdCliente = CC.IdCliente AND AA.FechaFoto = CC.FechaFoto
                    LEFT  JOIN (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 1 MONTH) AS FechaFoto, ID_CLIENTE AS IdCliente, MONTO_T_12M AS ComprasTottusU12M
                                FROM `bfa-cl-preapproved-dev.trf_bfa_cl_preapproved_dev.trf_preaprobados_transaccionalidad_tottus`
                                WHERE FECHA_FOTO >= '2020-12-01') DD ON AA.IdCliente = DD.IdCliente AND AA.FechaFoto = DD.FechaFoto

                    ),

    PD_CMR AS (SELECT FechaCarga AS FechaFoto, CtaContrato AS Contrato, PdFinal AS PdCMR,

                      CASE WHEN PdFinal <= 0.05 THEN '01. <= 5%'
                           WHEN PdFinal <= 0.10 THEN '02. >5 <=10%'
                           WHEN PdFinal <= 0.15 THEN '03. >10 <=15%'
                           WHEN PdFinal <= 0.20 THEN '04. >15 <=20%'
                           WHEN PdFinal <= 0.25 THEN '05. >20 <=25%'
                      ELSE '06. >25%' END AS TramoPdProvisiones,

               FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_veparis_cierre_cmr_prov` 
               WHERE FechaCarga >= '2020-12-01'
               ),

    INGRESOS_CLIENTE AS (SELECT DATE_ADD(FECHA_FOTO, INTERVAL 2 MONTH)         AS FechaFoto,
                                ID_CLIENTE                                     AS IdCliente,
                                RENTA_E17B2_BHV_TOP_PAT_R04LOG_FINAL           AS Renta,
                                FLAG_CLTE_BF_CMR_CON_INFO                      AS Flag,
                                RENTA_PILOTO                                   AS RentaPiloto,
                         
                         FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_customerbehavior_dev__trf_bfa_cl_ingresos_seguimiento_dev.svw_trf_ingresos_cliente_piloto` 
                         WHERE FECHA_FOTO>='2022-07-01'                        
                         ),

    AGRUPACION AS (SELECT AA.*,
                          BB.* EXCEPT (FechaFoto, IdCliente),
                          CC.* EXCEPT (FechaFoto, IdCliente),
                          DD.* EXCEPT (FechaFoto, IdCliente, Contrato),
                          EE.* EXCEPT (FechaFoto, IdCliente, Contrato),
                          FF.* EXCEPT (FechaFoto, IdCliente, SegmentoPreaprobados, ProbPreaprobados),
                          HH.* EXCEPT (FechaFoto, IdCliente),
                          II.* EXCEPT (FechaFoto, IdCliente),
                          JJ.* EXCEPT (FechaFoto, IdCliente),
                          KK.* EXCEPT (FechaFoto, IdCliente),
                          LL.* EXCEPT (IdCliente, Ranking),
                          MM.* EXCEPT (IdCliente, FechaFoto),
                          NN.PdCMR AS Pd_Provisiones,
                          NN.TramoPdProvisiones,
                          IFNULL(RentaPiloto,0) AS RentaPiloto,
                          IFNULL(PROB_REEST,ProbPreaprobados) PROB_REEST,
                          IFNULL(SegmentoPreaprobados_Reest,SegmentoPreaprobados) SegmentoPreaprobados,
                          SegmentoPreaprobados AS SegmentoPreaprobados_Antiguo,
                          ProbPreaprobados AS ProbPreaprobados_Antiguo,
                          IFNULL(PROB_REEST,ProbPreaprobados) ProbPreaprobados,
                          DATE_DIFF(FechaApertura, FechaSolicitud, DAY)                                                        AS AntiguedadSolicitud,
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 THEN 1 ELSE 0 END                      AS MarcaSolicitudMotorValida,
                          GREATEST(IFNULL(RentaSATIF, 0), IFNULL(RentaPreviredValidaU03M, 0))                                  AS RentaEvaluacion,
                          CASE WHEN IFNULL(DeudaConsumo  , 0) + IFNULL(LineaDisponible , 0) + IFNULL(DeudaComercial, 0) + IFNULL(DeudaHipotecario, 0) > 0 THEN 1 ELSE 0 END              AS MarcaBancarizadoU01M,
                          CASE WHEN RecuentoBancarizadoU06M >= 6 THEN 1 ELSE 0 END                                             AS MarcaBancarizadoU06M,
                          CASE WHEN DeudaConsumoSumU03M IS NULL OR (LineaDisponibleSumU03M + DeudaConsumoSumU03M) = 0 THEN -1
                          ELSE DeudaConsumoSumU03M / (LineaDisponibleSumU03M + DeudaConsumoSumU03M) END                        AS RatioDeudaLineaMasDeudaU03M,
                          CASE WHEN MarcaMora30Mes02 = 1 THEN DeudaTotalMes02 ELSE 0 END                                       AS DeudaMorosa30Mes02,
                          CASE WHEN MarcaMora30Mes03 = 1 THEN DeudaTotalMes03 ELSE 0 END                                       AS DeudaMorosa30Mes03,
                          CASE WHEN MarcaMora30Mes06 = 1 THEN DeudaTotalMes06 ELSE 0 END                                       AS DeudaMorosa30Mes06,
                          CASE WHEN MarcaMora60Mes06 = 1 THEN DeudaTotalMes06 ELSE 0 END                                       AS DeudaMorosa60Mes06,
                          CASE WHEN MarcaMora90Mes12 = 1 THEN DeudaTotalMes12 ELSE 0 END                                       AS DeudaMorosa90Mes12,
                          1                                                                                                    AS CasosTotal,
                          CASE WHEN MarcaMora30Mes02 = 1 THEN 1 ELSE 0 END                                                     AS Casos30Mes02,
                          CASE WHEN MarcaMora30Mes03 = 1 THEN 1 ELSE 0 END                                                     AS Casos30Mes03,
                          CASE WHEN MarcaMora30Mes06 = 1 THEN 1 ELSE 0 END                                                     AS Casos30Mes06,
                          CASE WHEN MarcaMora60Mes06 = 1 THEN 1 ELSE 0 END                                                     AS Casos60Mes06,
                          CASE WHEN MarcaMora90Mes12 = 1 THEN 1 ELSE 0 END                                                     AS Casos90Mes12,
                          IFNULL(DeudaConsumo, 0) * 0.035 + IFNULL(DeudaHipotecario, 0) / 240  + IFNULL(DeudaL1, 0) / 6 + IFNULL(DeudaL2, 0) / 24                                             AS CargaFinanciera,
                          CASE WHEN IFNULL(RentaEstimada, 0) > 0 THEN (IFNULL(DeudaConsumo, 0) * 0.035 + IFNULL(DeudaHipotecario, 0) / 240 + IFNULL(DeudaL1, 0) / 6 + IFNULL(DeudaL2, 0) / 24)/RentaEstimada ELSE -1 END AS DTI,
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 THEN IFNULL(AvaluoPropiedad, 0)
                               WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) >  60 THEN IFNULL(AvaluoTotal    , 0)
                               WHEN FechaSolicitud IS NULL                              THEN IFNULL(AvaluoTotal    , 0)
                               ELSE 0 END                                                                                           AS MontoPropiedad,
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 THEN IFNULL(TasacionVehiculo, 0)
                               WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) >  60 THEN IFNULL(TasacionTotal   , 0)
                               WHEN FechaSolicitud IS NULL                              THEN IFNULL(TasacionTotal   , 0)
                               ELSE 0 END                                                                                           AS MontoVehiculos,
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 THEN IFNULL(AvaluoPropiedad, 0) + IFNULL(TasacionVehiculo, 0)
                               WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) >  60 THEN IFNULL(AvaluoTotal    , 0) + IFNULL(TasacionTotal   , 0)
                               WHEN FechaSolicitud IS NULL                              THEN IFNULL(AvaluoTotal    , 0) + IFNULL(TasacionTotal   , 0)
                               ELSE 0 END                                                                                           AS MontoPatrimonio,
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 AND IFNULL(AvaluoPropiedad, 0) + IFNULL(TasacionVehiculo, 0) > 0 THEN 1
                               WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) >  60 AND IFNULL(AvaluoTotal    , 0) + IFNULL(TasacionTotal   , 0) > 0 THEN 1
                               WHEN FechaSolicitud IS NULL                              AND IFNULL(AvaluoTotal    , 0) + IFNULL(TasacionTotal   , 0) > 0 THEN 1
                               ELSE 0 END                                                                                           AS TenenciaPatrimonio,
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 AND IFNULL(NacionalidadMotor   , -1) = 152 THEN 'Chileno'
                               WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) >  60 AND IFNULL(NacionalidadSinacofi, -1) = 152 THEN 'Chileno'
                               WHEN FechaSolicitud IS NULL                              AND IFNULL(NacionalidadSinacofi, -1) = 152 THEN 'Chileno'
                               ELSE 'Extranjero' END                                                                                AS Nacionalidad,
                                  
                          CASE WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) <= 60 AND IFNULL(MarcaPreaprobadoMotor, 0) = 1 THEN 'Preaprobado'
                               WHEN DATE_DIFF(FechaApertura, FechaSolicitud, DAY) >  60 AND IFNULL(MarcaPreaprobadosBase, 0) = 1 THEN 'Preaprobado'
                               WHEN FechaSolicitud IS NULL                              AND IFNULL(MarcaPreaprobadosBase, 0) = 1 THEN 'Preaprobado'
                               ELSE 'Espontaneo' END                                                                                AS MarcaPreaprobado,
                          CASE --WHEN AA.FechaFoto <= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 3 MONTH) AND MesesConVintageMes03 < 3 THEN '01. Dado de baja - 3M'
                               --WHEN AA.FechaFoto <= DATE_SUB(DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH), MONTH), INTERVAL 6 MONTH) AND MesesConVintageMes06 < 6 THEN '02. Dado de baja - 6M'
                               WHEN IdSitu = 2                                                                            THEN '03. Suspendido'
                               WHEN IdSitu = 3                                                                            THEN '04. Castigado'
                               WHEN MarcaFraude = 1 AND DATE_DIFF(FechaFraude,AA.FechaFoto, MONTH) <= 12                  THEN '05. Fraude'
                               WHEN MarcaFraude2 = 1 AND DATE_DIFF(FechaFraude,AA.FechaFoto, MONTH) <= 12                 THEN '05. Fraude'
                               WHEN DATE_DIFF(AA.FechaFoto, FechaDefuncion, MONTH) >= 0                                   THEN "06. Fallecido"
                               ELSE 'Otros' END                                                                                     AS SegmentoAplicaVintage,
                          CASE WHEN IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/ > 0 THEN 1 
                               ELSE 0 END                                                                                           AS TenenciaRetailML,
                          ROW_NUMBER() OVER (PARTITION BY AA.Contrato, DATE_TRUNC(AA.FechaFoto, MONTH) ORDER BY FechaSolicitud DESC) AS Ranking,
                          CASE WHEN EdadMeses < 12*25                                                            THEN "Edad <25"
                               WHEN RecuentoBancarizadoU06M >= 6                                                 THEN "Bancarizado U06M"
                               WHEN IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/ > 0          THEN "NB U06M con info"
                               ELSE "NB U06M sin info" END AS SegmentoPoliticas,
                          CASE WHEN EE.IdCliente IS NULL THEN "Sin llamada al motor" ELSE "Con llamada al motor" END AS LlamadaMotor,
                          CASE WHEN DATE_DIFF(AA.FechaFoto, FechaDefuncion, MONTH) >= 0 THEN "Fallecido" ELSE "No Fallecido" END AS Fallecidos,
                                            
                   FROM STOCK                              AA
                   LEFT JOIN LOGICA_RENTA                  BB ON AA.IdCliente = BB.IdCliente AND AA.FechaFoto = BB.FechaFoto
                   LEFT JOIN R04                           CC ON AA.IdCliente = CC.IdCliente AND AA.FechaFoto = CC.FechaFoto
                   LEFT JOIN VINTAGE                       DD ON AA.Contrato  = DD.Contrato  AND AA.FechaFoto = DD.FechaFoto
                   LEFT JOIN MARCA_MOTOR                   EE ON AA.Contrato = EE.Contrato   AND AA.FechaFoto = EE.FechaFoto
                   LEFT JOIN ML_PREAP                      FF ON AA.IdCliente = FF.IdCliente AND AA.FechaFoto = FF.FechaFoto
                   LEFT JOIN MINIPOLITICA                  HH ON AA.IdCliente = HH.IdCliente AND AA.FechaFoto = HH.FechaFoto
                   LEFT JOIN VEHICULOS                     II ON AA.IdCliente = II.IdCliente AND AA.FechaFoto = II.FechaFoto
                   LEFT JOIN PROPIEDADES                   JJ ON AA.IdCliente = JJ.IdCliente AND AA.FechaFoto = JJ.FechaFoto
                   LEFT JOIN BASE_PREAPROBADOS             KK ON AA.IdCliente = KK.IdCliente AND AA.FechaFoto = KK.FechaFoto
                   LEFT JOIN SINACOFI                      LL ON AA.IdCliente = LL.IdCliente
                   LEFT JOIN INFO_RETAIL                   MM ON AA.IdCliente = MM.IdCliente AND AA.FechaFoto = MM.FechaFoto
                   LEFT JOIN PD_CMR                        NN ON AA.Contrato  = NN.Contrato  AND AA.FechaFoto = NN.FechaFoto
                   LEFT JOIN FRAUDE2                       OO ON AA.Contrato = OO.Contrato
                   LEFT JOIN ML_PREAP_NUEVO                PP ON AA.IdCliente = PP.IdCliente AND AA.FechaFoto = PP.FechaFoto
                   LEFT JOIN INGRESOS_CLIENTE              QQ ON AA.IdCliente = QQ.IdCliente AND AA.FechaFoto = QQ.FechaFoto
                   LEFT JOIN MARCA_RENE                    RR ON AA.Contrato = RR.Contrato 
                   
                   WHERE Reapertura IN("Reapertura","Originacion") AND IFNULL(Renegociacion,0) = 0
                   ),

    AGRUPACION2 AS(SELECT *, 
                          ProbPreaprobados AS ProbPreapFinal,
                          CASE WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND RentaEvaluacion>=300000 THEN DeudaConsumo/RentaEvaluacion ELSE NULL END              AS Leverage,
                          CASE WHEN IFNULL(MarcaBancarizadoU01M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'NB - SIN PAT - SIN RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'NB - CON PAT'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'NB - CON RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'NB - CON PAT - CON RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'B - SIN PAT - SIN RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'B - CON PAT'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'B - CON RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU01M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'B - CON PAT - CON RETAIL'
                          ELSE 'Otros' END AS SegmentosEvaluar,
                          CASE WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'NB - SIN PAT - SIN RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'NB - CON PAT'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'NB - CON RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'NB - CON PAT - CON RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'B - SIN PAT - SIN RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 0 THEN 'B - CON PAT'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 0 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'B - CON RETAIL'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND IFNULL(TenenciaPatrimonio, 0) = 1 AND IFNULL(TenenciaRetailML, 0) = 1 THEN 'B - CON PAT - CON RETAIL'
                          ELSE 'Otros' END AS SegmentosEvaluarU06M,
                          CASE WHEN IFNULL(TenenciaRetailML, 0) = 1 AND AperturaWeb = 1 THEN 'Retail - Digital'
                               WHEN IFNULL(TenenciaRetailML, 0) = 1 AND AperturaWeb = 0 THEN 'Retail - Presencial'
                               WHEN IFNULL(TenenciaRetailML, 0) = 0 AND AperturaWeb = 1 THEN 'No Retail - Digital'
                               WHEN IFNULL(TenenciaRetailML, 0) = 0 AND AperturaWeb = 0 THEN 'No Retail - Presencial'
                          ELSE 'Otros' END AS TipoSegmentoRetail,
                          CASE WHEN IFNULL(TenenciaPatrimonio, 0) = 1 AND AperturaWeb = 1 THEN 'Patrimonio - Digital'
                               WHEN IFNULL(TenenciaPatrimonio, 0) = 1 AND AperturaWeb = 0 THEN 'Patrimonio - Presencial'
                               WHEN IFNULL(TenenciaPatrimonio, 0) = 0 AND AperturaWeb = 1 THEN 'No Patrimonio - Digital'
                               WHEN IFNULL(TenenciaPatrimonio, 0) = 0 AND AperturaWeb = 0 THEN 'No Patrimonio - Presencial'
                          ELSE 'Otros' END AS TipoSegmentoPatrimonio,
                          CASE WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND AperturaWeb = 1 THEN 'Bancarizado - Digital'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND AperturaWeb = 0 THEN 'Bancarizado - Presencial'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND AperturaWeb = 1 THEN 'No Bancarizado - Digital'
                               WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND AperturaWeb = 0 THEN 'No Bancarizado - Presencial'
                          ELSE 'Otros' END AS TipoSegmentoBancarizado,
                          CASE WHEN IFNULL(RentaEvaluacion, 0) > 0 AND TenenciaPatrimonio = 1 THEN 'Renta y Patrimonio'
                               WHEN IFNULL(RentaEvaluacion, 0) = 0 AND TenenciaPatrimonio = 1 THEN 'Patrimonio'
                               WHEN IFNULL(RentaEvaluacion, 0) > 0 AND TenenciaPatrimonio = 0 THEN 'Renta'
                               WHEN IFNULL(RentaEvaluacion, 0) = 0 AND TenenciaPatrimonio = 0 THEN 'Sin renta ni patrimonio'
                          ELSE 'Otros' END                                                     AS SegmentoCliente,
                          CASE WHEN IFNULL(MontoVehiculos, 0) > 0 AND IFNULL(MontoPropiedad, 0) > 0 THEN 'Propiedad + Vehiculo'
                               WHEN IFNULL(MontoVehiculos, 0) > 0 AND IFNULL(MontoPropiedad, 0) = 0 THEN 'Vehiculo'
                               WHEN IFNULL(MontoVehiculos, 0) = 0 AND IFNULL(MontoPropiedad, 0) > 0 THEN 'Propiedad'
                               WHEN IFNULL(MontoVehiculos, 0) = 0 AND IFNULL(MontoPropiedad, 0) = 0 THEN 'Sin patrimonio'       
                          ELSE 'Otros' END                                                     AS TipoPatrimonio,
                          CASE WHEN IFNULL(DeudaConsumo, 0) = 0         THEN '00. Sin deuda'
                               WHEN IFNULL(DeudaConsumo, 0) < 100*1E3   THEN '01. < 100'
                               WHEN IFNULL(DeudaConsumo, 0) < 500*1E3   THEN '02. 100-500'
                               WHEN IFNULL(DeudaConsumo, 0) <   1*1E6   THEN '03. 500-1MM'
                               WHEN IFNULL(DeudaConsumo, 0) <   5*1E6   THEN '04. 1-5 MM'
                               WHEN IFNULL(DeudaConsumo, 0) <  10*1E6   THEN '05. 5-10MM'
                               WHEN IFNULL(DeudaConsumo, 0) <  20*1E6   THEN '06. 10-20MM'
                               WHEN IFNULL(DeudaConsumo, 0) <  30*1E6   THEN '07. 20-30MM'
                               WHEN IFNULL(DeudaConsumo, 0) <  40*1E6   THEN '08. 30-40MM'
                               WHEN IFNULL(DeudaConsumo, 0) <  50*1E6   THEN '09. 40-50MM'
                               WHEN IFNULL(DeudaConsumo, 0) >= 50*1E6   THEN '10. >= 50MM'
                          ELSE 'Otros' END                                                     AS TramoDeuda,
                          CASE WHEN RatioDeudaLineaMasDeudaU03M IS NULL THEN '00. Sin info'
                               WHEN RatioDeudaLineaMasDeudaU03M = -1    THEN '00. Sin info'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.1  THEN '01. <= 10%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.2  THEN '02. 10-20%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.3  THEN '03. 20-30%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.4  THEN '04. 30-40%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.5  THEN '05. 40-50%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.6  THEN '06. 50-60%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.7  THEN '07. 60-70%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.8  THEN '08. 70-80%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 0.9  THEN '09. 80-90%'
                               WHEN RatioDeudaLineaMasDeudaU03M <= 1.0  THEN '10. 90-100%'
                               WHEN RatioDeudaLineaMasDeudaU03M >  1.0  THEN '11. > 100%'
                          ELSE 'Otros' END                                                     AS TramoRatioDeudaLineaMasDeudaU03M,
                          CASE WHEN RentaEstimada = 0 THEN '00. Sin renta'
                               WHEN DTI = -1          THEN '00. Sin DTI'
                               WHEN DTI < 0.1         THEN '01. <= 10%'
                               WHEN DTI <= 0.2        THEN '02. 10-20%'
                               WHEN DTI <= 0.3        THEN '03. 20-30%'
                               WHEN DTI <= 0.4        THEN '04. 30-40%'
                               WHEN DTI <= 0.5        THEN '05. 40-50%'
                               WHEN DTI <= 0.6        THEN '06. 50-60%'
                               WHEN DTI <= 0.7        THEN '07. 60-70%'
                               WHEN DTI <= 0.8        THEN '08. 70-80%'
                               WHEN DTI <= 0.9        THEN '09. 80-90%'
                               WHEN DTI <= 1.0        THEN '10. 90-100%'
                               WHEN DTI <= 1.2        THEN '11. 100-120%'
                               WHEN DTI <= 1.5        THEN '12. 120-150%'
                               WHEN DTI <= 2.0        THEN '13. 150-200%'
                               WHEN DTI >  2.0        THEN '14. > 200%'
                          ELSE 'Otros' END                                                     AS TramoDTI,
                          CASE WHEN IFNULL(RentaEvaluacion, 0) <= 0 	       THEN '01. Sin renta'
                   	        WHEN IFNULL(RentaEvaluacion, 0) <= 300000   THEN '02. <= 300'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 400000  THEN '03. 300-=400'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 500000  THEN '04. 400-=500'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 600000  THEN '05. 500-=600'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 700000  THEN '06. 600-=700'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 800000  THEN '07. 700-=800'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 900000  THEN '08. 800-=900'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 1000000 THEN '09. 900-=1 MM'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 1200000 THEN '10. 1-=1.2 MM'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 1500000 THEN '11. 1.2-=1.5 MM'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 1700000 THEN '12. 1.5-=1.7 MM'
                   		    WHEN IFNULL(RentaEvaluacion, 0) <= 2000000 THEN '13. 1.7-=2.0 MM'
                   		    WHEN IFNULL(RentaEvaluacion, 0) >  2000000 THEN '14. > 2.0 MM'
                   	   ELSE 'Otros' END                                                    AS TramoRentaEvaluacion,
                          CASE WHEN IFNULL(RentaPiloto, 0) <= 0 	 THEN '01. Sin renta'
                   	       WHEN IFNULL(RentaPiloto, 0) <= 300000  THEN '02. <= 300'
                   		  WHEN IFNULL(RentaPiloto, 0) <= 500000  THEN '03. <=500'
                   		  WHEN IFNULL(RentaPiloto, 0) <= 800000  THEN '04. <=800'
                   		  WHEN IFNULL(RentaPiloto, 0) <= 1500000 THEN '05. <=1.5 MM'
                   		  WHEN IFNULL(RentaPiloto, 0) <= 2000000 THEN '06. <=2.0 MM'
                   		  WHEN IFNULL(RentaPiloto, 0) >  2000000 THEN '07. > 2.0 MM'
                   	       ELSE 'Otros' END                                                    AS TramoRentaPiloto,
                          CASE WHEN IFNULL(RentaEvaluacion, 0) > 0 THEN 1 ELSE 0 END           AS TenenciaRentaEvaluacion,
                          CASE WHEN IFNULL(RentaPreviredU03M, 0) > 0 THEN 1 ELSE 0 END         AS TenenciaRentaPreviredValida,
                          
                          CASE WHEN CupoCMR <= 0 	 THEN '01. Sin cupo'
                               WHEN CupoCMR <= 100000  THEN '02. <= 100'
                   	       WHEN CupoCMR <= 300000  THEN '03. 100-=300'
                   	       WHEN CupoCMR <= 400000  THEN '04. 300-=400'
                   	       WHEN CupoCMR <= 500000  THEN '05. 400-=500'
                   	       WHEN CupoCMR <= 600000  THEN '06. 500-=600'
                   	       WHEN CupoCMR <= 700000  THEN '07. 600-=700'
                   	       WHEN CupoCMR <= 800000  THEN '08. 700-=800'
                   	       WHEN CupoCMR <= 900000  THEN '09. 800-=900'
                   	       WHEN CupoCMR <= 1000000 THEN '10. 900-=1 MM'
                   	       WHEN CupoCMR <= 1200000 THEN '11. 1-=1.2 MM'
                   	       WHEN CupoCMR <= 1500000 THEN '12. 1.2-=1.5 MM'
                   	       WHEN CupoCMR <= 1700000 THEN '13. 1.5-=1.7 MM'
                   	       WHEN CupoCMR <= 2000000 THEN '14. 1.7-=2.0 MM'
                   	       WHEN CupoCMR >  2000000 THEN '15. > 2.0 MM'
                   	  ELSE 'Otros' END                                                     AS TramoCupo,
                          CASE WHEN EdadMeses < 21*12  THEN '01. < 21'
                               WHEN EdadMeses < 25*12  THEN '02. =21-25'
                               WHEN EdadMeses < 30*12  THEN '03. =25-30'
                               WHEN EdadMeses < 35*12  THEN '04. =30-35'
                               WHEN EdadMeses < 40*12  THEN '05. =35-40'
                               WHEN EdadMeses < 45*12  THEN '06. =40-45'
                               WHEN EdadMeses < 50*12  THEN '07. =45-50'
                               WHEN EdadMeses < 55*12  THEN '08. =50-55'
                               WHEN EdadMeses < 60*12  THEN '09. =55-60'
                               WHEN EdadMeses < 65*12  THEN '10. =60-65'
                               WHEN EdadMeses < 70*12  THEN '11. =65-70'
                               WHEN EdadMeses < 75*12  THEN '12. =70-75'
                               WHEN EdadMeses >= 75*12 THEN '13. >= 75'
                          ELSE 'Otros' END                                                    AS TramoEdad,
                          CASE WHEN MontoPatrimonio = 0         THEN '00. Sin patrimonio'
                               WHEN MontoPatrimonio <= 10*1E6   THEN '01. <= 10MM'
                               WHEN MontoPatrimonio <= 20*1E6   THEN '02. 10-20MM'
                               WHEN MontoPatrimonio <= 30*1E6   THEN '03. 20-30MM'
                               WHEN MontoPatrimonio <= 40*1E6   THEN '04. 30-40MM'
                               WHEN MontoPatrimonio <= 50*1E6   THEN '05. 40-50MM'
                               WHEN MontoPatrimonio <= 100*1E6  THEN '06. 50-100MM'
                               WHEN MontoPatrimonio >  100*1E6  THEN '07. > 100MM'
                          ELSE 'Otros' END                                                    AS TramoPatrimonio,
                   
                         CASE WHEN ProbPreaprobados_Antiguo>0.25 THEN "RECHAZO POLITICA MAYO22 PD>0.25"
                              WHEN IFNULL(RentaEvaluacion,0) > 0 AND IFNULL(RentaEvaluacion,0) < 400000 THEN "RECHAZO POLITICA MAYO22 RENTA"
                              WHEN TipoEmpleo = 'Dueña de Casa' THEN "RECHAZO POLITICA MAYO22 DUEÑA DE CASA"
                              WHEN EdadMeses < 21*12 THEN "RECHAZO POLITICA MAYO22 EDAD<21"
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados_Antiguo>0.15 THEN "RECHAZO POLITICA MAYO22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados_Antiguo > 0.25 THEN "RECHAZO POLITICA MAYO22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND ProbPreaprobados_Antiguo > 0.25 THEN "RECHAZO POLITICA MAYO22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados_Antiguo > 0.25 THEN "RECHAZO POLITICA MAYO22 NB SinInfo"
                   
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados_Antiguo > 0.25 THEN "RECHAZO MODELO MAYO22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados_Antiguo > 0.25 THEN "RECHAZO MODELO MAYO22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados_Antiguo > 0.25 THEN "RECHAZO MODELO MAYO22 EXTRANJERO NB"
                              WHEN ClasificacionPreaprobados = "MENOR 21" THEN "RECHAZO MODELO MAYO22 MENOR 21"
                   
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.10 THEN "RECHAZO POLITICA OCTUBRE22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados > 0.13 THEN "RECHAZO POLITICA OCTUBRE22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (TenenciaPatrimonio >0 OR TenenciaRetailML>0) AND ProbPreaprobados > 0.13 THEN "RECHAZO POLITICA OCTUBRE22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados > 0.10 THEN "RECHAZO POLITICA OCTUBRE22 NB SinInfo"
                   
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.10 THEN "RECHAZO MODELO OCTUBRE22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados > 0.10 THEN "RECHAZO MODELO OCTUBRE22 EXTRANJERO NB"
                              
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados > 0.12 THEN "RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND ProbPreaprobados > 0.12 THEN "RECHAZO POLITICA NOVIEMBRE22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA NOVIEMBRE22 NB SinInfo"
                   
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.10 THEN "RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados > 0.10 THEN "RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB"
                   
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Extranjero" THEN "RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Chileno" AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno"
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA FEBRERO23 EDAD<25"
                              WHEN EdadMeses < 25*12 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Extranjero" AND ProbPreaprobados > 0.07 THEN "RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero"
                              ELSE "APROBADO FEBRERO23" END AS CambiosEspontaneo,
                   
                   
                         CASE WHEN EdadMeses < 25*12 AND ProbPreaprobados_Antiguo > 0.15 THEN "RECHAZO POLITICA AGOSTO22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados_Antiguo > 0.20 THEN "RECHAZO POLITICA AGOSTO22 Bancarizado"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (TenenciaPatrimonio >0 OR TenenciaRetailML>0) AND ProbPreaprobados_Antiguo > 0.20 THEN "RECHAZO POLITICA AGOSTO22 NBConPatRet"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(TenenciaRetailML,0)=0) AND ProbPreaprobados_Antiguo > 0.16 THEN "RECHAZO POLITICA AGOSTO22 NBSinPatRet"
                   
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.11 THEN "RECHAZO POLITICA OCTUBRE22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados > 0.13 THEN "RECHAZO POLITICA OCTUBRE22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND ProbPreaprobados > 0.13 THEN "RECHAZO POLITICA OCTUBRE22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados > 0.10 THEN "RECHAZO POLITICA OCTUBRE22 NB SinInfo"
                   
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 EXTRANJERO NB"
                              WHEN ClasificacionPreaprobados = "MENOR 21" AND ProbPreaprobados > 0.09 THEN "RECHAZO MODELO OCTUBRE22 EDAD<21"
                              
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.10 THEN "RECHAZO POLITICA NOVIEMBRE22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados > 0.12 THEN "RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (TenenciaPatrimonio >0 OR TenenciaRetailML>0) AND ProbPreaprobados > 0.12 THEN "RECHAZO POLITICA NOVIEMBRE22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA NOVIEMBRE22 NB SinInfo"
                              
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB"
                              WHEN ClasificacionPreaprobados = "MENOR 21" AND ProbPreaprobados > 0.08 THEN "RECHAZO MODELO NOVIEMBRE22 EDAD<21"
                              
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Extranjero" THEN "RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Chileno" AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno"
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA FEBRERO23 EDAD<25"
                              WHEN EdadMeses < 25*12 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Extranjero" AND ProbPreaprobados > 0.07 THEN "RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero"
                              ELSE "APROBADO FEBRERO23" END AS CambiosPreaprobado,
                   
                          CASE WHEN EdadMeses < 25*12 AND ProbPreaprobados_Antiguo > 0.15 THEN "RECHAZO POLITICA AGOSTO22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados_Antiguo > 0.20 THEN "RECHAZO POLITICA AGOSTO22 Bancarizado"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (TenenciaPatrimonio >0 OR TenenciaRetailML>0) AND ProbPreaprobados_Antiguo > 0.20 THEN "RECHAZO POLITICA AGOSTO22 NBConPatRet"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(TenenciaRetailML,0)=0) AND ProbPreaprobados_Antiguo > 0.16 THEN "RECHAZO POLITICA AGOSTO22 NBSinPatRet"
                   
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.11 THEN "RECHAZO POLITICA OCTUBRE22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados > 0.13 THEN "RECHAZO POLITICA OCTUBRE22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND ProbPreaprobados > 0.13 THEN "RECHAZO POLITICA OCTUBRE22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados > 0.10 THEN "RECHAZO POLITICA OCTUBRE22 NB SinInfo"
                   
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados > 0.13 THEN "RECHAZO MODELO OCTUBRE22 EXTRANJERO NB"
                              WHEN ClasificacionPreaprobados = "MENOR 21" AND ProbPreaprobados > 0.09 THEN "RECHAZO MODELO OCTUBRE22 EDAD<21"
                              
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.10 THEN "RECHAZO POLITICA NOVIEMBRE22 EDAD<25"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND ProbPreaprobados > 0.12 THEN "RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (TenenciaPatrimonio >0 OR TenenciaRetailML>0) AND ProbPreaprobados > 0.12 THEN "RECHAZO POLITICA NOVIEMBRE22 NB ConInfo"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 0 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA NOVIEMBRE22 NB SinInfo"
                              
                              WHEN ClasificacionPreaprobados = "BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 BANCARIZADO"
                              WHEN ClasificacionPreaprobados = "NO BANCARIZADO MAYOR 21" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO"
                              WHEN ClasificacionPreaprobados = "EXTRANJERO NO BANCARIZADO" AND ProbPreaprobados > 0.12 THEN "RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB"
                              WHEN ClasificacionPreaprobados = "MENOR 21" AND ProbPreaprobados > 0.08 THEN "RECHAZO MODELO NOVIEMBRE22 EDAD<21"
                              
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Extranjero" THEN "RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero"
                              WHEN IFNULL(MarcaBancarizadoU06M, 0) = 1 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Chileno" AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno"
                              WHEN EdadMeses < 25*12 AND ProbPreaprobados > 0.09 THEN "RECHAZO POLITICA FEBRERO23 EDAD<25"
                              WHEN EdadMeses < 25*12 AND (IFNULL(TenenciaPatrimonio,0) =0 AND IFNULL(ComprasSodimacU12M, 0) + IFNULL(ComprasFalabellaU12M, 0) /*+ IFNULL(ComprasTottusU12M, 0)*/=0) AND Nacionalidad = "Extranjero" AND ProbPreaprobados > 0.07 THEN "RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero"
                              ELSE "APROBADO FEBRERO23" END AS CambiosPreaprobadosinTotus,
                   
                   FROM AGRUPACION
                   )

SELECT *,
       CASE WHEN MarcaPreaprobado="Espontaneo" AND FechaFoto >= '2022-05-01' AND CambiosEspontaneo IN("RECHAZO POLITICA MAYO22 PD>0.25","RECHAZO POLITICA MAYO22 EDAD<21","RECHAZO POLITICA MAYO22 RENTA","RECHAZOMAYO22 SEGMENTO","RECHAZO POLITICA MAYO22 DUEÑA DE CASA","RECHAZO POLITICA MAYO22 BANCARIZADO","RECHAZO       POLITICA MAYO22 NB ConInfo","RECHAZO POLITICA MAYO22 NB SinInfo","RECHAZO MODELO MAYO22 BANCARIZADO","RECHAZO MODELO MAYO22 NO BANCARIZADO","RECHAZO MODELO MAYO22 EXTRANJERO NB","RECHAZO MODELO MAYO22 MENOR 21","RECHAZO POLITICA MAYO22 EDAD<25") THEN "RECHAZADO MAYO22 FECHAS"
            WHEN MarcaPreaprobado="Espontaneo" AND FechaFoto >= '2022-10-01' AND CambiosEspontaneo IN("RECHAZO MODELO OCTUBRE22 BANCARIZADO","RECHAZO MODELO OCTUBRE22 NOBANCARIZADO","RECHAZO MODELO OCTUBRE22 EXTRANJERO NB","RECHAZO POLITICA OCTUBRE22 EDAD<25",
                 "RECHAZO POLITICA OCTUBRE22 BANCARIZADO","RECHAZO POLITICA OCTUBRE22 NB ConInfo","RECHAZO POLITICA OCTUBRE22 NB SinInfo") THEN "RECHAZADO OCTUBRE22 FECHAS"
            WHEN MarcaPreaprobado="Espontaneo" AND FechaFoto >= '2022-11-01' AND CambiosEspontaneo IN("RECHAZO MODELO NOVIEMBRE22 BANCARIZADO","RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO","RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB","RECHAZO POLITICA NOVIEMBRE22 EDAD<25",
                 "RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO","RECHAZO POLITICA NOVIEMBRE22 NB ConInfo","RECHAZO POLITICA NOVIEMBRE22 NB SinInfo") THEN "RECHAZADO NOVIEMBRE22 FECHAS"
            WHEN MarcaPreaprobado="Espontaneo" AND FechaFoto >= '2023-02-01' AND CambiosEspontaneo IN("RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero","RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno","RECHAZO POLITICA FEBRERO23 EDAD<25","RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero") THEN "RECHAZADO        FEBRERO23 FECHAS"
            WHEN MarcaPreaprobado="Espontaneo" AND CambiosEspontaneo IN("RECHAZO POLITICA MAYO22 PD>0.25","RECHAZO POLITICA MAYO22 EDAD<21","RECHAZO POLITICA MAYO22 RENTA","RECHAZOMAYO22 SEGMENTO","RECHAZO POLITICA MAYO22 DUEÑA DE CASA","RECHAZO POLITICA MAYO22 BANCARIZADO","RECHAZO POLITICA MAYO22 NB ConInfo",      "RECHAZO POLITICA MAYO22 NB SinInfo","RECHAZO MODELO MAYO22 BANCARIZADO","RECHAZO MODELO MAYO22 NO BANCARIZADO","RECHAZO MODELO MAYO22 EXTRANJERO NB","RECHAZO MODELO MAYO22 MENOR 21","RECHAZO POLITICA MAYO22 EDAD<25") THEN "RECHAZADO MAYO22"
            WHEN MarcaPreaprobado="Espontaneo" AND CambiosEspontaneo IN("RECHAZO MODELO OCTUBRE22 BANCARIZADO","RECHAZO MODELO OCTUBRE22 NOBANCARIZADO","RECHAZO MODELO OCTUBRE22 EXTRANJERO NB","RECHAZO POLITICA OCTUBRE22 EDAD<25",
                 "RECHAZO POLITICA OCTUBRE22 BANCARIZADO","RECHAZO POLITICA OCTUBRE22 NB ConInfo","RECHAZO POLITICA OCTUBRE22 NB SinInfo") THEN "RECHAZADO OCTUBRE22"
            WHEN MarcaPreaprobado="Espontaneo" AND CambiosEspontaneo IN("RECHAZO MODELO NOVIEMBRE22 BANCARIZADO","RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO","RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB","RECHAZO POLITICA NOVIEMBRE22 EDAD<25",
                 "RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO","RECHAZO POLITICA NOVIEMBRE22 NB ConInfo","RECHAZO POLITICA NOVIEMBRE22 NB SinInfo") THEN "RECHAZADO NOVIEMBRE22"
            WHEN MarcaPreaprobado="Espontaneo" AND CambiosEspontaneo IN("RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero","RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno","RECHAZO POLITICA FEBRERO23 EDAD<25","RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero") THEN "RECHAZADO FEBRERO23"
            WHEN MarcaPreaprobado="Espontaneo" THEN "APROBADO"
       
            WHEN MarcaPreaprobado="Preaprobado" AND FechaFoto >= '2022-08-01' AND CambiosPreaprobadosinTotus IN("RECHAZO POLITICA AGOSTO22 EDAD<25","RECHAZO POLITICA AGOSTO22 Bancarizado","RECHAZO POLITICA AGOSTO22 NBConPatRet","RECHAZO POLITICA AGOSTO22 NBSinPatRet") THEN "RECHAZADO AGOSTO22 FECHAS"
            WHEN MarcaPreaprobado="Preaprobado" AND FechaFoto >= '2022-10-01' AND CambiosPreaprobadosinTotus IN("RECHAZO MODELO OCTUBRE22 BANCARIZADO","RECHAZO MODELO OCTUBRE22 NOBANCARIZADO","RECHAZO MODELO OCTUBRE22 EXTRANJERO NB","RECHAZO MODELO OCTUBRE22 EDAD<21",
                 "RECHAZO POLITICA OCTUBRE22 EDAD<25","RECHAZO POLITICA OCTUBRE22 BANCARIZADO","RECHAZO POLITICA OCTUBRE22 NB ConInfo","RECHAZO POLITICA OCTUBRE22 NB SinInfo") THEN "RECHAZADO OCTUBRE22 FECHAS"
            WHEN MarcaPreaprobado="Preaprobado" AND FechaFoto >= '2022-11-01' AND CambiosPreaprobadosinTotus IN("RECHAZO MODELO NOVIEMBRE22 BANCARIZADO","RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO","RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB","RECHAZO MODELO NOVIEMBRE22 EDAD<21",
                 "RECHAZO POLITICA NOVIEMBRE22 EDAD<25","RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO","RECHAZO POLITICA NOVIEMBRE22 NB ConInfo","RECHAZO POLITICA NOVIEMBRE22 NB SinInfo") THEN "RECHAZADO NOVIEMBRE22 FECHAS"
            WHEN MarcaPreaprobado="Preaprobado" AND FechaFoto >= '2023-02-01' AND CambiosPreaprobadosinTotus IN("RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero","RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno","RECHAZO POLITICA FEBRERO23 EDAD<25","RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero") THEN       "RECHAZADO FEBRERO23 FECHAS"
            WHEN MarcaPreaprobado="Preaprobado" AND CambiosPreaprobadosinTotus IN("RECHAZO POLITICA AGOSTO22 EDAD<25","RECHAZO POLITICA AGOSTO22 Bancarizado","RECHAZO POLITICA AGOSTO22 NBConPatRet","RECHAZO POLITICA AGOSTO22 NBSinPatRet") THEN "RECHAZADO AGOSTO22"
            WHEN MarcaPreaprobado="Preaprobado" AND CambiosPreaprobadosinTotus IN("RECHAZO MODELO OCTUBRE22 BANCARIZADO","RECHAZO MODELO OCTUBRE22 NOBANCARIZADO","RECHAZO MODELO OCTUBRE22 EXTRANJERO NB","RECHAZO MODELO OCTUBRE22 EDAD<21",
                 "RECHAZO POLITICA OCTUBRE22 EDAD<25","RECHAZO POLITICA OCTUBRE22 BANCARIZADO","RECHAZO POLITICA OCTUBRE22 NB ConInfo","RECHAZO POLITICA OCTUBRE22 NB SinInfo") THEN "RECHAZADO OCTUBRE22"
            WHEN MarcaPreaprobado="Preaprobado" AND CambiosPreaprobadosinTotus IN("RECHAZO MODELO NOVIEMBRE22 BANCARIZADO","RECHAZO MODELO NOVIEMBRE22 NOBANCARIZADO","RECHAZO MODELO NOVIEMBRE22 EXTRANJERO NB","RECHAZO MODELO NOVIEMBRE22 EDAD<21",
                 "RECHAZO POLITICA NOVIEMBRE22 EDAD<25","RECHAZO POLITICA NOVIEMBRE22 BANCARIZADO","RECHAZO POLITICA NOVIEMBRE22 NB ConInfo","RECHAZO POLITICA NOVIEMBRE22 NB SinInfo") THEN "RECHAZADO NOVIEMBRE22"
            WHEN MarcaPreaprobado="Preaprobado" AND CambiosPreaprobadosinTotus IN("RECHAZO POLITICA FEBRERO23 NB SinInfo Extranjero","RECHAZO POLITICA FEBRERO23 NB SinInfo Chileno","RECHAZO POLITICA FEBRERO23 EDAD<25","RECHAZO POLITICA FEBRERO23 EDAD<25 SinInfo Extranjero") THEN "RECHAZADO FEBRERO23"
            WHEN MarcaPreaprobado="Preaprobado" THEN "APROBADO"
       ELSE "OTROS" END AS CambiosTotal,
       
       FROM AGRUPACION2

;

CREATE OR REPLACE TABLE `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.VN_seguimiento_cartera_cmr_Agrupado` AS

SELECT  FechaFoto, COUNT(Contrato) N, TipoEmpleo, TenenciaRetailML, SUM(DeudaTotalMes03) DeudaTotalMes03, SUM(DeudaTotalMes06) DeudaTotalMes06, TramoProbabilidadPreap, SegmentoPreaprobados, SUM(ProbPreapFinal) ProbPreapFinal, SUM(Pd_Provisiones) Pd_Provisiones, SUM(Leverage) AS Leverage, SUM(CupoCMR) CupoCMR, TramoPdProvisiones, SUM(DeudaMorosa30Mes03) DeudaMorosa30Mes03, SUM(DeudaMorosa30Mes06) DeudaMorosa30Mes06, SUM(DeudaMorosa60Mes06) DeudaMorosa60Mes06, MarcaPreaprobado, TramoRentaEvaluacion, TramoEdad, TipoSegmentoBancarizado, SegmentoAplicaVintage, TramoCupo,	TramoPatrimonio

FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.trf_seguimiento_cartera_cmr`

GROUP BY 1,3,4,7,8,13,17,18,19,20,21,22,23

;

CREATE OR REPLACE TABLE `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.VN_seguimiento_cartera_cmr_conCambios_Agrupado`
AS
SELECT  FechaFoto, COUNT(Contrato) N, TipoEmpleo, TenenciaRetailML, SUM(DeudaTotalMes03) DeudaTotalMes03, SUM(DeudaTotalMes06) DeudaTotalMes06, TramoProbabilidadPreap, SegmentoPreaprobados, SUM(ProbPreapFinal) ProbPreapFinal, SUM(Pd_Provisiones) Pd_Provisiones, SUM(Leverage) AS Leverage, SUM(CupoCMR) CupoCMR, TramoPdProvisiones, SUM(DeudaMorosa30Mes03) DeudaMorosa30Mes03, SUM(DeudaMorosa30Mes06) DeudaMorosa30Mes06, SUM(DeudaMorosa60Mes06) DeudaMorosa60Mes06, MarcaPreaprobado, TramoRentaEvaluacion, TramoEdad, TipoSegmentoBancarizado, SegmentoAplicaVintage, TramoCupo,TramoPatrimonio, CambiosTotal,


FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.trf_seguimiento_cartera_cmr`

GROUP BY 1,3,4,7,8,13,17,18,19,20,21,22,23,24


