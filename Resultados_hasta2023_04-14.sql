	
WITH AC_FINTOC AS (SELECT RUT AS IdCliente, 1 AS Aumento_Fintoc,
                   FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.VN_Fintoc_Aumentos_Etapa1`

                   UNION DISTINCT
                   
                   SELECT RUT AS IdCliente, 1 AS Aumento_Fintoc,
                   FROM `bfa-cl-risk-dev.trf_bfa_cl_risk_portfolio_dev.VN_Fintoc_Aumentos_Etapa2`
                   ),
	
     MAESTRA AS (SELECT *, DATE_TRUNC(FechaCarga, MONTH) AS FechaFoto,
                 FROM `fif-bfa-cl-risk-discovery.svw_bfa_cl_datalake_prd__trf_bfa_cl_datalake_prd.svw_trf_contrato_maestra_riesgo_fcon` 
                 WHERE FechaCarga IN('2022-06-30','2022-07-31','2022-08-31','2022-09-30','2022-10-31','2022-11-30','2022-12-31','2023-01-31','2023-02-28','2023-03-31','2023-04-30')
                 ),

     AUMENTOS_DISMINUCIONES AS (SELECT FechaFoto,
                                       Contrato,
                                       IdCliente,
                                       LAG(FCupo) OVER (PARTITION BY Contrato ORDER BY FechaFoto ASC) AS FCupoAnterior,
                                       FCupo AS FCupoActual,

                                       CASE WHEN FCupo >  LAG(FCupo) OVER (PARTITION BY Contrato ORDER BY FechaFoto ASC) THEN "Aumento"
                                            WHEN FCupo <  LAG(FCupo) OVER (PARTITION BY Contrato ORDER BY FechaFoto ASC) THEN "Disminuyo"
                                            ELSE "" END AS CambioCupo

                                FROM MAESTRA
                                ),

     AUMENTOS AS (SELECT *
                  FROM (SELECT FechaFoto,
                               IdCliente,
                               Contrato,
                               1 AS Aumento,
                               FCupoAnterior,
                               FCupoActual,
                               ROW_NUMBER() OVER(PARTITION BY IdCliente ORDER BY FechaFoto ASC) AS Ranking

                        FROM AUMENTOS_DISMINUCIONES
                        WHERE CambioCupo = "Aumento"
                        )
                  WHERE Ranking=1
                  ),

     AGRUPACION AS (SELECT AA.FechaFoto AS FechaAumento,
                           CC.FechaFoto,
                           AA.IdCliente,
                           AA.Contrato,
                           FCupoAnterior,
                           FCupoActual,
                           FCupoActual-FCupoAnterior                                AS MontoAumento,
                           (FCupoActual-FCupoAnterior)/FCupoAnterior*100            AS Porc_Aumento,
                           IFNULL(Aumento_Fintoc,0)                                 AS Aumento_Fintoc,

                           
                           CASE WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) = -1  THEN "Mmenos1"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  0  THEN "M0"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  1  THEN "M1"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  2  THEN "M2"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  3  THEN "M3"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  4  THEN "M4"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  5  THEN "M5"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  6  THEN "M6"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  7  THEN "M7"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  8  THEN "M8"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  9  THEN "M9"
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  10 THEN "M10"
                                ELSE "M_Otros" END AS Mes,
                           
                           CASE WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) = -1  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  0  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  1  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  2  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  3  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  4  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  5  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  6  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  7  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  8  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  9  THEN FSaldeud
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  10 THEN FSaldeud
                                ELSE 0 END AS DeudaL1,
                           
                           CASE WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) = -1  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  0  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  1  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  2  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  3  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  4  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  5  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  6  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  7  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  8  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  9  THEN FSaldeud/FCupo
                                WHEN DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) =  10 THEN FSaldeud/FCupo
                                ELSE 0 END AS Utilizacion,

                    FROM AUMENTOS AA

                    LEFT JOIN AC_FINTOC BB USING (IdCliente)
                    LEFT JOIN MAESTRA   CC ON AA.Contrato = CC.Contrato AND DATE_DIFF(CC.FechaFoto, AA.FechaFoto, MONTH) >=-1

                    ),

     RESUMEN AS (SELECT *
                 FROM (SELECT FechaAumento, IdCliente, FCupoAnterior, FCupoActual, MontoAumento, Porc_Aumento, DeudaL1, Utilizacion, Mes, Aumento_Fintoc FROM AGRUPACION)
                 PIVOT(COUNT(IdCliente) N, SUM(FCupoAnterior) FCupoAnterior, SUM(FCupoActual) FCupoActual, SUM(MontoAumento) MontoAumento, SUM(Porc_Aumento) Porc_Aumento, SUM(DeudaL1) DeudaL1, SUM(Utilizacion) Utilizacion  FOR Mes IN("Mmenos1","M0","M1","M2","M3","M4","M5","M6","M7","M8","M9","M10"))
                 ORDER BY FechaAumento
                 )

SELECT FechaAumento,
       Aumento_Fintoc,
       N_Mmenos1 AS N,
       FCupoAnterior_Mmenos1 AS CupoAnterior,       
       FCupoActual_Mmenos1   AS CupoActual,
       MontoAumento_Mmenos1  AS MontoAumento,
       Porc_Aumento_Mmenos1  AS Porc_Aumento,

       DeudaL1_Mmenos1,
       DeudaL1_M0,
       DeudaL1_M1,
       DeudaL1_M2,
       DeudaL1_M3,
       DeudaL1_M4,
       DeudaL1_M5,
       DeudaL1_M6,
       DeudaL1_M7,
       DeudaL1_M8,
       DeudaL1_M9,
       DeudaL1_M10,
       
       Utilizacion_Mmenos1,
       Utilizacion_M0,
       Utilizacion_M1,
       Utilizacion_M2,
       Utilizacion_M3,
       Utilizacion_M4,
       Utilizacion_M5,
       Utilizacion_M6,
       Utilizacion_M7,
       Utilizacion_M8,
       Utilizacion_M9,
       Utilizacion_M10,

FROM RESUMEN
WHERE Aumento_Fintoc = 1
--WHERE Aumento_Fintoc = 0

