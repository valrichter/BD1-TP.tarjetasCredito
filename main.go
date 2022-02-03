package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

var db, _ = sql.Open("postgres", "user=postgres host=localhost dbname=negocio sslmode=disable")

func main() {

	defer db.Close()

	var option int = -1

	for option != 0 {
		fmt.Println()
		fmt.Println("1. Crear base de datos")
		fmt.Println("2. Crear tablas")
		fmt.Println("3, Crear PK's Y FK's")
		fmt.Println("4. Eliminar PK's Y FK's")
		fmt.Println("5. Cargar tablas")
		fmt.Println("6. Cargar Consumos")
		fmt.Println("7. Crear Stored Procedures y Triggers")
		fmt.Println("8. Probar Consumos")
		fmt.Println("9. Generar Resumen")
		fmt.Println("10. Cargar datos NoSQL en BolDB")
		fmt.Println("0. Salir")
		fmt.Print("Ingresar accion: ")

		fmt.Scan(&option)

		if option == 1 {
			crearDataBase()
		}
		if option == 2 {
			crearTablas()
		}
		if option == 3 {
			crearPKsFKs()
		}
		if option == 4 {
			borrarPKsFKs()
		}
		if option == 5 {
			cargarTablas()
		}
		if option == 6 {
			cargarConsumos()
		}
		if option == 7 {
			crearSPyTriggers()
		}
		if option == 8 {
			pasarCosasAcompraORechazo()
		}
		if option == 9 {
			generarResumenes()
		}
		if option == 10 {
			MainNOSQL()
		}
		if option == 0 {
			//salir()
		}
	}
}

//-------------------------------------------------------------------------------------crearDataBase()------------------
func crearDataBase() {
	dbDefault, err := sql.Open("postgres", "user=postgres host=localhost dbname=postgres sslmode=disable") // conexion a psql
	if err != nil {
		log.Fatal(err)
	}

	defer dbDefault.Close()
	fmt.Println("Conectado con Postgres")

	_, err = dbDefault.Exec(`drop database if exists negocio`)
	if err != nil {
		log.Fatal(err)
	}
	_, err = dbDefault.Exec(`create database negocio`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'Base de datos creada'\n")
}

//-------------------------------------------------------------------------------------crearTablas()--------------------
func crearTablas() {
	_, err := db.Exec(`	CREATE TABLE cliente( nrocliente serial, nombre text, apellido text, domicilio text, telefono char(12));
						CREATE TABLE tarjeta( nrotarjeta char(16), nrocliente int, validadesde char(6), validahasta char(6), codseguridad char(4),limitecompra decimal(8,2), estado char(10) );	
						CREATE TABLE comercio(nrocomercio int, nombre text,domicilio text,codigopostal char(8),telefono char(12));		
						CREATE TABLE compra( nrooperacion serial, nrotarjeta char(16),nrocomercio int,fecha timestamp, monto decimal(7,2), pagado boolean);	
						CREATE TABLE rechazo( nrorechazo serial,nrotarjeta char(16),nrocomercio int,fecha timestamp,monto decimal(7,2),motivo text);
						CREATE TABLE cierre(año int,mes int,terminacion int, fechainicio date,fechacierre date,fechavto date);
						CREATE TABLE cabecera(nroresumen serial,nombre text,apellido text,domicilio text,nrotarjeta char(16), desde date, hasta date,vence date,total decimal(8,2));
						CREATE TABLE detalle(nroresumen int, nrolinea int, fecha date, nombrecomercio text, monto decimal(7,2));
						CREATE TABLE alerta(nroalerta serial,nrotarjeta char(16),fecha timestamp,nrorechazo int,codalerta int, descripcion text);
						CREATE TABLE consumo(nrotarjeta char(16),codseguridad char(4),nrocomercio int, monto decimal(7,2));
						`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'Tablas creadas'\n")
}

//-------------------------------------------------------------------------------------crearPKsFKs()--------------------
func crearPKsFKs() {
	_, err := db.Query(`
						ALTER TABLE cliente     ADD CONSTRAINT cliente_pk   PRIMARY KEY (nrocliente);
						ALTER TABLE tarjeta     ADD CONSTRAINT tarjeta_pk   PRIMARY KEY (nrotarjeta);
						ALTER TABLE comercio    ADD CONSTRAINT comercio_pk  PRIMARY KEY (nrocomercio);
						ALTER TABLE compra      ADD CONSTRAINT compra_pk    PRIMARY KEY (nrooperacion);
						ALTER TABLE rechazo     ADD CONSTRAINT rechazo_pk   PRIMARY KEY (nrorechazo);
						ALTER TABLE cierre      ADD CONSTRAINT cierre_pk    PRIMARY KEY (año, mes, terminacion);
						ALTER TABLE cabecera    ADD CONSTRAINT cabecera_pk  PRIMARY KEY (nroresumen);
						ALTER TABLE detalle     ADD CONSTRAINT detalle_pk   PRIMARY KEY (nroresumen, nrolinea);	
						ALTER TABLE alerta      ADD CONSTRAINT alerta_pk    PRIMARY KEY (nroalerta);
						ALTER TABLE consumo    	ADD CONSTRAINT consumo_pk  	PRIMARY KEY (nrotarjeta, codseguridad, nrocomercio, monto); --todo es pk creo						
						
						
						ALTER TABLE tarjeta     ADD CONSTRAINT tarjeta_nrocliente_fk    FOREIGN KEY (nrocliente)    REFERENCES cliente(nrocliente);
						ALTER TABLE compra      ADD CONSTRAINT compra_nrotarjeta_fk     FOREIGN KEY (nrotarjeta)    REFERENCES tarjeta(nrotarjeta);
						ALTER TABLE compra      ADD CONSTRAINT compra_nrocomercio_fk    FOREIGN KEY (nrocomercio)   REFERENCES comercio(nrocomercio);
						ALTER TABLE rechazo     ADD CONSTRAINT rechazo_nrotarjeta_fk    FOREIGN KEY (nrotarjeta)    REFERENCES tarjeta(nrotarjeta);
						ALTER TABLE rechazo     ADD CONSTRAINT rechazo_nrocomercio_fk   FOREIGN KEY (nrocomercio)   REFERENCES comercio(nrocomercio);
						ALTER TABLE cabecera    ADD CONSTRAINT cabecera_nrotarjeta_fk   FOREIGN KEY (nrotarjeta)    REFERENCES tarjeta(nrotarjeta);
						ALTER TABLE detalle     ADD CONSTRAINT detalle_nroresumen_fk    FOREIGN KEY (nroresumen)    REFERENCES cabecera(nroresumen);
						ALTER TABLE alerta      ADD CONSTRAINT alerta_nrotarjeta_fk     FOREIGN KEY (nrotarjeta)    REFERENCES tarjeta(nrotarjeta);
						ALTER TABLE alerta      ADD CONSTRAINT alerta_nrorechazo_fk     FOREIGN KEY (nrorechazo)    REFERENCES rechazo(nrorechazo);
						ALTER TABLE consumo     ADD CONSTRAINT consumo_nrocomercio_fk  	FOREIGN KEY (nrocomercio)   REFERENCES comercio(nrocomercio);`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'PKs y FKs creadas'\n")
}

//-------------------------------------------------------------------------------------borrarPKsFKs()-------------------
func borrarPKsFKs() {
	_, err := db.Query(`
						ALTER TABLE cierre DROP CONSTRAINT cierre_pk; 
								
						ALTER TABLE alerta DROP CONSTRAINT alerta_nrotarjeta_fk;
						ALTER TABLE alerta DROP CONSTRAINT alerta_nrorechazo_fk;
						ALTER TABLE alerta DROP CONSTRAINT alerta_pk; 
					
						ALTER TABLE detalle DROP CONSTRAINT detalle_nroresumen_fk; 
						ALTER TABLE detalle DROP CONSTRAINT detalle_pk;
						
						ALTER TABLE cabecera DROP CONSTRAINT cabecera_nrotarjeta_fk;
						ALTER TABLE cabecera DROP CONSTRAINT cabecera_pk;
				
						ALTER TABLE rechazo DROP CONSTRAINT rechazo_nrocomercio_fk;
						ALTER TABLE rechazo DROP CONSTRAINT rechazo_nrotarjeta_fk;
						ALTER TABLE rechazo DROP constraint rechazo_pk;
				
						ALTER TABLE compra DROP constraint compra_nrocomercio_fk;
						ALTER TABLE compra DROP constraint compra_nrotarjeta_fk;
						ALTER TABLE compra DROP constraint compra_pk;

						ALTER TABLE consumo DROP constraint consumo_nrocomercio_fk;
						ALTER TABLE consumo DROP constraint consumo_pk;
						
						ALTER TABLE comercio DROP constraint comercio_pk;
				
						ALTER TABLE tarjeta DROP constraint tarjeta_nrocliente_fk;
						ALTER TABLE tarjeta DROP constraint tarjeta_pk;
				
						ALTER TABLE cliente DROP constraint cliente_pk;
						`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'PKs y FKs eliminadas'\n")
}

//-------------------------------------------------------------------------------------cargarTablas()-------------------
func cargarTablas() {
	cargarClientes()
	cargarTarjetas()
	cargarComercios()
	cargarCierresSP()

	fmt.Print("'TABLAS CARGADAS: cliente, tarjeta, comercio, cierre'\n")
}

func cargarClientes() {
	_, err := db.Exec(`INSERT INTO cliente VALUES(1,'Juan','Pereira','Alfaro Rosas 1225','11-3570-6626');
						INSERT INTO cliente VALUES(2,'Lucia','Romano','Cura Brochero 6779','11-0425-3377');
						INSERT INTO cliente VALUES(3,'Romina','Torres','General San Martin 1903','11-4154-6895');
						INSERT INTO cliente VALUES(4,'Carlos','Torres','General Perez 681','11-1523-2757');
						INSERT INTO cliente VALUES(5,'Jose','Zaracho','Caballito 582','11-5957-9596');
						INSERT INTO cliente VALUES(6,'Osvaldo','Gimenez','9 de Julio','11-8427-8552'); --tarjeta expirada (6)
						INSERT INTO cliente VALUES(7,'Lucrecia','Lucero','27 de Febrero 8891','11-8172-0764');
						INSERT INTO cliente VALUES(8,'Rosalia','Patiño','Parque Chacabuco 4212','11-5154-5267');
						INSERT INTO cliente VALUES(9,'Eric','Almeida','Flores 7225','11-1838-5524'); 
						INSERT INTO cliente VALUES(10,'Monica','Alarcon','San Cristobal 456','11-6617-4876');
						INSERT INTO cliente VALUES(11,'Ramon','Araoz','Monte Castro 897','11-4144-0557');
						INSERT INTO cliente VALUES(12,'Angel','Campo','Monte Castros 3212','11-7092-3691');
						INSERT INTO cliente VALUES(13,'Geronimo','Castaño','Villa Luro 3486','11-6334-1063');
						INSERT INTO cliente VALUES(14,'Zulma','Ibañez','Versalles 2219','11-1824-0025'); 
						INSERT INTO cliente VALUES(15,'Luz','Espeche','Parque Chacabuco 721','11-5118-8851'); --tarjeta expirada (15)
						INSERT INTO cliente VALUES(16,'Joel','Teves','Nueva Pompeya 4544','11-3201-8470');
						INSERT INTO cliente VALUES(17,'Romelia','Gimenez','Nueva Pompeya 985','11-0080-3730');
						INSERT INTO cliente VALUES(18,'Estela','Trujillo','Villa del Parque 1688','11-7112-9988');
						INSERT INTO cliente VALUES(19,'Gladys','Urquiza','Caballito 3567','11-8641-5502');
						INSERT INTO cliente VALUES(20,'Flavio','Olivieri','San Nicolás 801','11-4451-6909');
					`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'Clientes cargados'\n")
}

func cargarTarjetas() {
	_, err := db.Exec(`INSERT INTO tarjeta VALUES('5397853252415070', 1 , '201106', '202401', 'GPXL', 100000.90 ,'vigente');
						INSERT INTO tarjeta VALUES('4819499406355990', 2 , '201009', '202305', 'ABEQ', 9000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('3762989859397151', 3 , '201002', '202309', 'PVLM', 200000.10 ,'suspendida');--suspendida
						INSERT INTO tarjeta VALUES('4930773436893711', 4 , '202001', '202510', 'PQBS', 100500.20 ,'vigente');
						INSERT INTO tarjeta VALUES('6011479189073212', 5 , '201102', '202601', 'LNMM', 29999.99 ,'vigente');

						INSERT INTO tarjeta VALUES('6011511477319402', 6 , '200101', '202001', 'MNBJ', 34000.00 ,'vencida'); -- vencida
						INSERT INTO tarjeta VALUES('5567040693959913', 6 , '201007', '202305', 'JOXA', 80000.21 ,'vigente');

						INSERT INTO tarjeta VALUES('5180412218008103', 7 , '201801', '202409', 'KCML', 900000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('3498209616290204', 8 , '201710', '202412', 'PBWW', 999999.99 ,'vigente');
						INSERT INTO tarjeta VALUES('3744994489819124', 9 , '202006', '202409', 'HJGH', 99192.23 ,'vigente');
						INSERT INTO tarjeta VALUES('5163771537049065', 10, '201901', '202210', 'GRES', 30000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('6011939729021625', 11, '201905', '202407', 'ABCS', 50000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('4073730340914896', 12, '201505', '202407', 'PKVI', 594850.00 ,'vigente');
						INSERT INTO tarjeta VALUES('5204048187238426', 13, '202107', '202412', 'EVXQ', 70000.00 ,'vigente');

						INSERT INTO tarjeta VALUES('3438945192209197', 14, '201405', '202401', 'LMCN', 700000.00 ,'vencida'); --vencida
						INSERT INTO tarjeta VALUES('3548186086585857', 14, '200303', '202311', 'KFMV', 120000.00 ,'vigente');

						INSERT INTO tarjeta VALUES('4212254408240238', 15, '201701', '201901', 'VPLM', 100000.00 ,'vigente'); 

						INSERT INTO tarjeta VALUES('4587905767429348', 16, '202101', '202405', 'NVHE', 400000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('3424221903251379', 17, '202109', '202407', 'MXHE', 30000.20  ,'suspendida'); --suspendida
						INSERT INTO tarjeta VALUES('3480254282191239', 18, '201909', '202410', 'VPEM', 600000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('7002658458051430', 19, '201503', '202605', 'GBLS', 140000.00 ,'vigente');
						INSERT INTO tarjeta VALUES('5860454284520931', 20, '201503', '202605', 'LCDL', 110000.18 ,'vigente');
						`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'Tarjetas cargadas'\n")
}

func cargarComercios() {
	_, err := db.Exec(`INSERT INTO comercio VALUES(1,'coto', 'dean funes 5321', 'B1675', '11-4323-3212');
						INSERT INTO comercio VALUES(2,'carrefour', 'alvear 1350', 'B1703', '11-4242-6342');
						INSERT INTO comercio VALUES(3,'adidas store', 'rivadavia 5433', 'B1751', '11-1231-5344');
						INSERT INTO comercio VALUES(4,'yenny', 'yrigoyen 8623', 'B1824', '11-8675-9454');
						INSERT INTO comercio VALUES(5,'zara', 'bocayuva 9019', 'B1827', '11-8654-3124');
						INSERT INTO comercio VALUES(6,'open 25', 'mexico 8572', 'B2942', '11-3276-9123');
						INSERT INTO comercio VALUES(7,'guerrin', 'rawson 1321', 'B8109', '11-4264-8723');
						INSERT INTO comercio VALUES(8,'el gato negro', 'alsina 9281', 'C1001', '11-1236-9571');
						INSERT INTO comercio VALUES(9,'burger king', 'moreno 8712', 'C1002', '11-8267-3125');
						INSERT INTO comercio VALUES(10,'ferreteria don kcho', 'mitre 2645', 'C1003', '11-8572-0272');
						INSERT INTO comercio VALUES(11,'old bridge', 'sarmiento 8543', 'C1004', '11-8562-9572');
						INSERT INTO comercio VALUES(12,'panaderia las delicias', 'sarandi 9353', 'C1005', '11-7572-8562');
						INSERT INTO comercio VALUES(13,'barber shop', 'alsina 2940', 'C1005', '11-8272-2344');
						INSERT INTO comercio VALUES(14,'el molino', 'chile 9751', 'C1007', '11-0292-5272');
						INSERT INTO comercio VALUES(15,'kfc', 'solis 7451', 'C1008', '11-4827-9257');
						INSERT INTO comercio VALUES(16,'el ateneo', 'pasteur 1812', 'C1009', '11-7562-8272');
						INSERT INTO comercio VALUES(17,'jumbo', 'tucuman 2311', 'C1010', '11-8572-9572');
						INSERT INTO comercio VALUES(18,'nike store', 'callao 1818', 'C1011', '11-7562-8276');
						INSERT INTO comercio VALUES(19,'hoyts cinema', 'cabello 1012', 'C1012', '11-8262-9383');
						INSERT INTO comercio VALUES(20,'green company', 'laprida 9182', 'C1013', '11-8424-8252');
						`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'Comercios cargados'\n")
}

func cargarCierresSP() {
	_, err := db.Query(`CREATE OR REPLACE FUNCTION cargar_cierres() returns void as $$
							DECLARE
								finicio	date := '2021-01-05';
								fcierre	date := finicio + 30;
								fvto	date := fcierre + 11;
								terminacion int;
								mes int;
								
							BEGIN		
								FOR mes IN 1 .. 12 LOOP 
									FOR terminacion IN 0 .. 9 LOOP 		
										INSERT INTO cierre VALUES(2021, mes, terminacion, finicio, fcierre, fvto);
										finicio := finicio + 1;
										fcierre := fcierre + 1;
										fvto := fvto + 1;
									END LOOP;
									finicio := (finicio - 9) + 30;
									fcierre := finicio + 30;
									fvto := fcierre + 11;
								END LOOP;		
							END;
							$$ LANGUAGE plpgsql;
					`)
	if err != nil {
		log.Fatal(err)
	}
	db.Exec(`SELECT cargar_cierres();`)

	fmt.Print("'Cierres cargados'\n")
}

//-------------------------------------------------------------------------------------cargarConsumos()-----------------
func cargarConsumos() {
	_, err := db.Exec(`
			-- cliente 2
			INSERT INTO consumo values ('4819499406355990', 'ABEQ', 1, 3000.00); --compra
			INSERT INTO consumo values ('4819499406355990', 'ABEQ', 1, 10000.00);--
			INSERT INTO consumo values ('4819499406355990', 'ABEQ', 2, 7000.00); --
			
			INSERT INTO consumo values ('4819499406355990', 'ABEQ', 2, 8000.00); 
			INSERT INTO consumo values ('4819499406355990', 'ABEQ', 3, 2000.00); 
			
			-- sin cliente
			INSERT INTO consumo values ('1234123412341234', 'ABEQ', 3, 2000.00); 

			-- cliente 6
			INSERT INTO consumo values ('6011511477319402', 'MNBJ', 7, 2000.00); 
			INSERT INTO consumo values ('6011511477319402', 'MNBJ', 10, 8000.00); 

			-- cliente 17
			INSERT INTO consumo values ('3424221903251379', 'MXHE', 7, 2000.00); 
								        
			-- cliente 1 
			INSERT INTO consumo values ('5397853252415070', 'GPXL', 11, 10000.00); 
			INSERT INTO consumo values ('5397853252415070', 'GPXL', 19, 50000.00);  
			INSERT INTO consumo values ('5397853252415070', 'GPXL', 5, 90000.00); 
			INSERT INTO consumo values ('5397853252415070', 'GPXL', 6, 80000.00); 
			INSERT INTO consumo values ('5397853252415070', 'GPXL', 5, 1000.00); 

			-- cliente 14 
			
			INSERT INTO consumo values ('3438945192209197', 'LMCN', 11, 20000.00); -- vencida, no entra	
			INSERT INTO consumo values ('3548186086585857', 'KFMV', 19, 10000.00); -- este entra	
			INSERT INTO consumo values ('3548186086585857', 'KFMV', 5, 90000.00); -- este entra	
			INSERT INTO consumo values ('3548186086585857', 'KFMV', 5, 1000.00); -- entra, resumen =  101000

		
			INSERT INTO consumo values ('4587905767429348', 'AAAA', 3, 1000.00); --rechazo

			--cliente 15 dos comrpas mismo codigo postal
			INSERT INTO consumo values ('4212254408240238', 'VPLM', 12, 20000.00); --compra
			INSERT INTO consumo values ('4212254408240238', 'VPLM', 13, 1000.00); --compra
			
			`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'Consumos cargados'\n")
}

//-------------------------------------------------------------------------------------crearSPyTriggers()---------------
func crearSPyTriggers() {
	alertaClienteTrigger()
	alertasComprasTrigger()
	comprasPendientesDePagoSP()
	autorizarCompraSP()
	simularPasarConsumosAcompraORechazoSP()
	generarResumenSP()

	fmt.Print("'SP's y TRIGGERS CREADOS'\n")
}

//--------------------------------------------------------------------------------comprasPendientesDePagoSP()---------------
func comprasPendientesDePagoSP() {
	_, err := db.Query(`
		CREATE OR REPLACE FUNCTION compras_pendientes_de_pago(i_nrotarjeta char) returns int as $$
			DECLARE
				montoSumado int = 0;
				compra_tarjeta record;
				
			BEGIN
				FOR compra_tarjeta IN SELECT * FROM compra WHERE nrotarjeta = i_nrotarjeta AND pagado = false loop
					montoSumado = montoSumado + compra_tarjeta.monto;
				END LOOP;
			
				RETURN montoSumado;
			END;
			$$ LANGUAGE plpgsql;
		`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'compras_pendientes_de_pago SP creado'\n")
}

//--------------------------------------------------------------------------------autorizarCompraSP()---------------
func autorizarCompraSP() {
	_, err := db.Query(`
		CREATE OR REPLACE FUNCTION autorizar_compra(i_n_tarjeta char, i_cod_seguridad char,i_n_comercio int, i_monto int) returns  boolean as $$
			DECLARE
				tarjeta_ingresada record;
				fhoy date;
			
			BEGIN
				fhoy := now(); 
				SELECT * INTO tarjeta_ingresada FROM tarjeta WHERE nrotarjeta = i_n_tarjeta;

			
				IF NOT FOUND THEN 
					
					INSERT INTO tarjeta VALUES (i_n_tarjeta, 1, null, null, null, null, 'anulada'); 
					INSERT INTO rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo)
					VALUES (i_n_tarjeta, i_n_comercio, fhoy, i_monto, 'tarjeta no valida');				
					RETURN false;
				END IF;
		
				
				IF tarjeta_ingresada.codseguridad != i_cod_seguridad THEN
					INSERT INTO rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo)
					VALUES (i_n_tarjeta, i_n_comercio, fhoy, i_monto, 'codigo de seguridad invalido');
					RETURN false;
				END IF;
				
				
				IF tarjeta_ingresada.limitecompra < compras_pendientes_de_pago(tarjeta_ingresada.nrotarjeta) + i_monto THEN
					INSERT INTO rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo) 
					VALUES (i_n_tarjeta, i_n_comercio, fhoy,i_monto, 'supera limite de compra');
					RETURN false;
				END IF;
				
				
				IF tarjeta_ingresada.estado = 'vencida   ' THEN --existe pero esta vencida
					 INSERT INTO rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo)
					 VALUES (i_n_tarjeta, i_n_comercio, fhoy, i_monto, 'plazo de vigencia expirado');
					 RETURN false;
				END IF;
		
				
				IF tarjeta_ingresada.estado = 'suspendida' THEN
					 INSERT INTO rechazo (nrotarjeta, nrocomercio, fecha, monto, motivo)
					 VALUES (i_n_tarjeta, i_n_comercio, fhoy, i_monto, 'la tarjeta se encuentra suspendida');
					 RETURN false;
				END IF;
				
			
				RETURN true;
			END;
			$$ LANGUAGE plpgsql;`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'autorizar_compra SP creado'\n")

}

//--------------------------------------------------------------------simularPasarConsumosAcompraORechazoSP y probarConsumos()-----------------

func simularPasarConsumosAcompraORechazoSP() {
	_, err := db.Query(`
		CREATE OR REPLACE FUNCTION simular_pasar_consumos_a_compra_o_rechazo() returns void as $$
			DECLARE
				consumo record;
				fhoy date = localtimestamp;
				
			BEGIN
				for consumo in select * from consumo loop
					if(autorizar_compra(consumo.nrotarjeta, consumo.codseguridad, consumo.nrocomercio, consumo.monto :: int) = true) then
						 insert into compra (nrotarjeta, nrocomercio, fecha, monto, pagado)
						 values (consumo.nrotarjeta, consumo.nrocomercio, fhoy, consumo.monto,false);
					end if;
				end loop;
			END;
		$$ LANGUAGE plpgsql;
		`)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Print("'simular_pasar_consumos_a_compra_o_rechazo SP creado'\n")
}

func pasarCosasAcompraORechazo() {
	db.Exec(`SELECT simular_pasar_consumos_a_compra_o_rechazo();`)

}

//-------------------------------------------------------------------------------------resumenes SP y generarResumenes()---------------

func generarResumenSP() {
	_, err := db.Query(`
		CREATE OR REPLACE FUNCTION generacion_de_resumen(i_nro_cliente int, i_año int, i_mes int) returns void as $$
			DECLARE	
				tarjetaAux record; 
				compraAux record; 
				total decimal(8,2) = 0.0; 
				clienteAux record; 
				terminacion_tarjeta int; 
				cierreAux record; 
				comercioAux record; 
				cabeceraAux record; 
				nrolineaAux int = 0; 
				fAux date;
				
			BEGIN
				SELECT * INTO clienteAux FROM cliente WHERE nrocliente = i_nro_cliente;

				FOR tarjetaAux IN SELECT * FROM tarjeta WHERE nrocliente = i_nro_cliente loop
					terminacion_tarjeta := substring(tarjetaAux.nrotarjeta from '.$');
		
					SELECT * INTO cierreAux FROM cierre WHERE terminacion = terminacion_tarjeta AND año = i_año AND mes = i_mes;
				
				
					FOR compraAux IN SELECT * FROM compra WHERE nrotarjeta = tarjetaAux.nrotarjeta AND pagado = false loop
	
						fAux = compraAux.fecha::date ;
						IF fAux <= cierreAux.fechacierre and fAux > cierreAux.fechainicio THEN
						
						
							total := total + compraAux.monto;	
												
						END IF;	
												
					END loop;
					
		
					INSERT INTO cabecera (nombre, apellido, domicilio, nrotarjeta, desde, hasta, vence, total)
					VALUES (clienteAux.nombre, clienteAux.apellido, clienteAux.domicilio, tarjetaAux.nrotarjeta,
							cierreAux.fechainicio, cierreAux.fechacierre, cierreAux.fechavto, total);
		
					SELECT * INTO cabeceraAux FROM cabecera ORDER BY nroresumen DESC;
					
				
					FOR compraAux IN SELECT * FROM compra WHERE nrotarjeta = tarjetaAux.nrotarjeta AND pagado = false loop
						IF fAux <= cierreAux.fechacierre and fAux > cierreAux.fechainicio THEN
							SELECT * INTO comercioAux FROM comercio WHERE nrocomercio = compraAux.nrocomercio;
							nrolineaAux = nrolineaAux + 1;
		
							INSERT INTO detalle VALUES (cabeceraAux.nroresumen, nrolineaAux, compraAux.fecha, comercioAux.nombre, compraAux.monto);		
						END IF;						
					END loop;
					
					total := 0;
					nrolineaAux := 0;
						
				END loop;
		END;
		$$ LANGUAGE plpgsql;
		`)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Print("'generacion_de_resumen SP creado'\n")
}

func generarResumenes() {
	_, err := db.Query(`
		CREATE OR REPLACE FUNCTION generar_resumenes() returns void as $$
			BEGIN
				PERFORM	generacion_de_resumen(1, 2021, 11); 
				PERFORM generacion_de_resumen(2, 2021, 11); 
				PERFORM generacion_de_resumen(14, 2021, 10); 
			
				PERFORM generacion_de_resumen(15, 2021, 10); 
		END;
		$$ LANGUAGE plpgsql;
		`)
	if err != nil {
		log.Fatal(err)
	}

	db.Exec(`SELECT generar_resumenes();`)
	fmt.Print("'Resumenes generados'\n")
}

//------------------------------------------------------------------------------------alertas_clientesTrigger y alerta_compras()----------------------

func alertaClienteTrigger() {
	_, err := db.Query(`CREATE OR REPLACE FUNCTION t_a() returns TRIGGER as $$
					DECLARE
						alertaAux record;
						cantRechazos int = 0;	
							
					BEGIN
					
						SELECT COUNT(*) INTO cantRechazos FROM rechazo WHERE nrotarjeta = new.nrotarjeta
										AND date_part('day', fecha) = date_part('day', new.fecha)
										AND motivo = 'supera limite de compra';							

						IF cantRechazos >= 2 THEN			
							INSERT INTO alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion)		
							VALUES (new.nrotarjeta, new.fecha, new.nrorechazo, 32, 'limite');			

							UPDATE tarjeta SET estado = 'suspendida' WHERE nrotarjeta = new.nrotarjeta;	
		
						ELSE	
							INSERT INTO alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion)	
							VALUES (new.nrotarjeta, new.fecha, new.nrorechazo, 0, 'rechazo');		
						END IF;
						
						return new;
					END;
$$ language plpgsql;
						
	CREATE TRIGGER t_a_trigger
		AFTER INSERT on rechazo
		FOR each row
		execute PROCEDURE t_a();
						`)
	if err != nil {
		log.Fatal(err)
	}

}

func alertasComprasTrigger() {
	_, err := db.Query(`CREATE OR REPLACE FUNCTION alertas_a_compras() returns TRIGGER as $$
						DECLARE
							compraAux record;
							tiempo int;
							comercioAux record;
							comercioNew record;
							
						BEGIN
							FOR compraAux IN SELECT * FROM compra WHERE nrotarjeta = new.nrotarjeta loop
								tiempo := EXTRACT(EPOCH FROM (compraAux.fecha - new.fecha::timestamp));
								SELECT * INTO comercioAux FROM comercio WHERE nrocomercio = compraAux.nrocomercio;
									
								
								SELECT * INTO comercioNew FROM comercio WHERE nrocomercio = new.nrocomercio;
									
								
								IF tiempo < 60 AND comercioAux.nrocomercio != comercioNew.nrocomercio 
									AND comercioAux.codigopostal = comercioNew.codigopostal THEN
									INSERT INTO alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion)
									VALUES (new.nrotarjeta, new.fecha, null, 1, 'compra 1min');
								END IF;
								
								IF tiempo < 60*5 AND comercioAux.codigopostal != comercioNew.codigopostal THEN
									INSERT INTO alerta (nrotarjeta, fecha, nrorechazo, codalerta, descripcion)
									VALUES (new.nrotarjeta, new.fecha, null, 5, 'compra 5min');
								END IF;
							END loop;
						
							RETURN new;
						END;
						$$ language plpgsql;
						
						CREATE TRIGGER alertas_a_compras_trg
						AFTER INSERT on compra
						FOR each row
						execute PROCEDURE alertas_a_compras();
						`)
	if err != nil {
		log.Fatal(err)
	}

}
