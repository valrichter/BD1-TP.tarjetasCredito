package main

import (
	"encoding/json"
	"fmt"
	bolt "go.etcd.io/bbolt"
	"log"
	"strconv"
	"time"
)

type Cliente struct {
	NroCliente int
	Nombre     string
	Apellido   string
	Domicilio  string
	Telefono   string
}

type Tarjeta struct {
	NroTarjeta   string
	NroCliente   int
	ValidaDesde  string
	ValidaHasta  string
	CodSeguridad string
	LimiteCompra float64
	Estado       string
}

type Comercio struct {
	NroComercio int
	Nombre      string
	Domicilio   string
	CodPostal   string
	Telefono    string
}

type Compra struct {
	NroOperacion int
	NroTarjeta   string
	NroComercio  int
	Fecha        time.Time
	Monto        float64
	Motivo       bool
}

func CreateUpdate(db *bolt.DB, bucketName string, key []byte, val []byte) error {
	//abre transaccion de escritura
	tx, err := db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	b, _ := tx.CreateBucketIfNotExists([]byte(bucketName))

	err = b.Put(key, val)
	if err != nil {
		return err
	}
	//Cierra transaccion
	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

func ReadUnique(db *bolt.DB, bucketName string, key []byte) ([]byte, error) {
	var buf []byte
	//abre una transaccion de lectura
	err := db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		buf = b.Get(key)
		return nil
	})
	return buf, err
}

func MainNOSQL() {
	db, err := bolt.Open("guarani.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var option = -1

	for option != 0 {
		fmt.Println()
		fmt.Println("Base de Datos NOSQL")
		fmt.Println()
		fmt.Println("1. Cargar y mostrar datos NOSQL")
		fmt.Println("2. Generar Compras")
		fmt.Println("0. Volver")
		fmt.Scan(&option)
		if option == 1 {
			cargarClientesNSQL(db)
			cargarTarjetasNSQL(db)
			cargarComerciosNSQL(db)

		}
		if option == 2 {
			generarComprasNSQL(db)
		}
		if option == 0 {
			fmt.Println()
		}
	}

}

func cargarClientesNSQL(db *bolt.DB) {

	cliente1 := Cliente{1, "Juan", "Pereira", "Alfaro Rosas 1225", "11-3570-6626"}
	cliente2 := Cliente{2, "Lucia", "Romano", "Cura Brochero 6779", "11-0425-3377"}
	cliente3 := Cliente{3, "Romina", "Torres", "General San Martin 1903", "11-4154-6895"}

	data1, err := json.Marshal(cliente1)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "cliente1", []byte(strconv.Itoa(cliente1.NroCliente)), data1)

	data2, err := json.Marshal(cliente2)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "cliente2", []byte(strconv.Itoa(cliente2.NroCliente)), data2)

	data3, err := json.Marshal(cliente3)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "cliente3", []byte(strconv.Itoa(cliente3.NroCliente)), data3)

	resultado1, err := ReadUnique(db, "cliente1", []byte(strconv.Itoa(cliente1.NroCliente)))
	resultado2, err := ReadUnique(db, "cliente2", []byte(strconv.Itoa(cliente2.NroCliente)))
	resultado3, err := ReadUnique(db, "cliente3", []byte(strconv.Itoa(cliente3.NroCliente)))
	fmt.Println()
	fmt.Printf("Clientes")
	fmt.Println()
	fmt.Printf("%s\n", resultado1)
	fmt.Printf("%s\n", resultado2)
	fmt.Printf("%s\n", resultado3)
}

func cargarTarjetasNSQL(db *bolt.DB) {
	tarjeta1 := Tarjeta{"5397853252415070", 1, "201106", "202401", "GPXL", 100000.90, "vigente"}
	tarjeta2 := Tarjeta{"4819499406355990", 2, "201009", "202305", "ABEQ", 9000.32, "vigente"}
	tarjeta3 := Tarjeta{"3762989859397151", 3, "201002", "202309", "PVML", 200000.43, "vigente"}

	data1, err := json.Marshal(tarjeta1)
	if err != nil {
		log.Fatal(err)

	}
	CreateUpdate(db, "tarjeta1", []byte(tarjeta1.NroTarjeta), data1)

	data2, err := json.Marshal(tarjeta2)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "tarjeta2", []byte(tarjeta2.NroTarjeta), data2)

	data3, err := json.Marshal(tarjeta3)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "tarjeta3", []byte(tarjeta3.NroTarjeta), data3)

	resultado1, err := ReadUnique(db, "tarjeta1", []byte(tarjeta1.NroTarjeta))
	resultado2, err := ReadUnique(db, "tarjeta2", []byte(tarjeta2.NroTarjeta))
	resultado3, err := ReadUnique(db, "tarjeta3", []byte(tarjeta3.NroTarjeta))

	fmt.Println()
	fmt.Printf("Tarjetas")
	fmt.Println()
	fmt.Printf("%s\n", resultado1)
	fmt.Printf("%s\n", resultado2)
	fmt.Printf("%s\n", resultado3)
}

func cargarComerciosNSQL(db *bolt.DB) {
	comercio1 := Comercio{1, "Coto", "Dean Funes 5321", "B1675", "11-4323-3212"}
	comercio2 := Comercio{2, "Carrefour", "Alvear 1350", "B1703", "11-4242-6342"}
	comercio3 := Comercio{3, "Adidas Store", "Rivadavia 5433", "B1751", "11-1231-5344"}

	data1, err := json.Marshal(comercio1)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "comercio1", []byte(strconv.Itoa(comercio1.NroComercio)), data1)

	data2, err := json.Marshal(comercio2)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "comercio2", []byte(strconv.Itoa(comercio2.NroComercio)), data2)

	data3, err := json.Marshal(comercio3)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "comercio3", []byte(strconv.Itoa(comercio3.NroComercio)), data3)

	resultado1, err := ReadUnique(db, "comercio1", []byte(strconv.Itoa(comercio1.NroComercio)))
	resultado2, err := ReadUnique(db, "comercio2", []byte(strconv.Itoa(comercio2.NroComercio)))
	resultado3, err := ReadUnique(db, "comercio3", []byte(strconv.Itoa(comercio3.NroComercio)))
	fmt.Println()
	fmt.Printf("Comercios")
	fmt.Println()
	fmt.Printf("%s\n", resultado1)
	fmt.Printf("%s\n", resultado2)
	fmt.Printf("%s\n", resultado3)
}

func generarComprasNSQL(db *bolt.DB) {
	var ahora time.Time
	ahora = time.Now()

	compra1 := Compra{1, "5397853252415070", 2, ahora, 500, false}
	compra2 := Compra{2, "3762989859397151", 1, ahora, 640, false}
	compra3 := Compra{3, "5397853252415070", 3, ahora, 1050, false}

	data1, err := json.Marshal(compra1)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "compra1", []byte(strconv.Itoa(compra1.NroOperacion)), data1)

	data2, err := json.Marshal(compra2)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "compra2", []byte(strconv.Itoa(compra2.NroOperacion)), data2)

	data3, err := json.Marshal(compra3)
	if err != nil {
		log.Fatal(err)
	}
	CreateUpdate(db, "compra3", []byte(strconv.Itoa(compra3.NroOperacion)), data3)

	resultado1, err := ReadUnique(db, "compra1", []byte(strconv.Itoa(compra1.NroOperacion)))
	resultado2, err := ReadUnique(db, "compra2", []byte(strconv.Itoa(compra2.NroOperacion)))
	resultado3, err := ReadUnique(db, "compra3", []byte(strconv.Itoa(compra3.NroOperacion)))
	fmt.Println()
	fmt.Printf("Compras")
	fmt.Println()
	fmt.Printf("%s\n", resultado1)
	fmt.Printf("%s\n", resultado2)
	fmt.Printf("%s\n", resultado3)
}
