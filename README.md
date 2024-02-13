# Bases De Datos I: Trabajo Practico

El trabajo práctico tiene como objetivo implementar una pequeña base de datos encargada de almacenar y consultar informacion relacionada a la compras realizadas con tarjetas de credito por distintos clientes.\
Para mas informacion leer el archivo informe.asciidoc

## EJECUCION EN LINUX

1. Levantar el la app y el postgres

   ```text
   make docker_up
   ```

2. Correr la app

   ```text
   go run main.go
   ```

- Algunas funciones tienen explicacion dentro del codigo

## Introducción

El trabajo práctico tiene como objetivo implementar una pequeña base de datos encargada de almacenar y consultar
informacion relacionada a la compras realizadas con  tarjetas de credito por distintos clientes, como estas
pueden o no interactuar con los mismo y los diferentes negocios involucrados en el sistema. Contaremos con las tablas ```cliente```, ```tarjeta```, ```comercio```, ```compra```, ```rechazo```, ```cierre```, ```cabecera```, ```detalle```, ```alerta```, y por ultimo ```consumo```.
Las tablas ```cliente```, ```tarjeta```, ```comercio``` y ```cierre``` contendran informacion desde su creacion. Las demas iran almacenando informacion dinamicamente dada diferentes relaciones y respetando pautas entre las tablas ya cargadas.

## Descripción

### 1. Decisiones tomadas

o primero a destacar es que vimos conveniente cambiar en la tabla ```cliente```  el tipo de dato int en la llave primaria ```nrocliente```
por un serial (puede verse en los primeros commits), ya que lo necesitabamos para una funcion,luego volvimos a dejarlo
en int porque la funcion implementada quedo descartada.

En la tabla ```compra``` optamos por cambiar el tipo de la llave primaria ```nrooperacion``` de int a serial, ya que en principio
no sabriamos el ```nrooperacion``` que sigue cuando se genera una compra (esto es dinamico) y ademas es unico, tener un
contador no podria ser una buena opcion para resolver este problema en una base de datos.

Del mismo modo en la tabla ```rechazo```, optamos por cambiar el tipo de la llave primaria ```nrorechazo``` de int a serial.

En la tabla ```alerta``` repetimos esta decision ya que si se generan automaticamente registros en la tabla, no sabriamos
por que ```nroalerta``` iria (llave primaria) y como ya nombramos, no seria buena idea tener un contador.

En la tabla ```rechazo``` implementamos una sola FK (```nronegocio```), la misma es utilizada directamente por la funcion
`validar```tarjeta` (que esta presente tanto en ```main.go``` como en el archivo ```sql```, se puede probar en los distintos
archivos por separado) que verifica que exista una tarjeta.La FK ```nronegocio``` es unica ya que si un negocio
no existe no deberia haber ahi un consumo.

### 2. Problemas que solucionamos

Dificultad en la  asignacion de valores para las claves primarias cada vez que se genera un nuevo registro, optamos por implementar el tipo de dato ```serial```, que se encarga de completar dinamicamente los valores de las PK.

### 3. Dificultades

Tuvimos un problema con la conexion entre modulos de go pero pudimos solucionarlo. Se generaron dudas con la relacion entre archivos que existian en diferentes directorios y que no podian comunicarse entre si, optamos por tener un unico directorio y colocar los archivos de la creacion y manipulacion de la base de datos.

## Implementación

La implementacion del programa se lleva a cabo sobre el archivo ```main.go```. Aqui se encuentran las funciones necesarias
para la creacion de la Base de Datos, las tablas, la implementacion y eliminacion de las llaves primarias y foraneas,
con las mismas tambien podemos cargar las tablas mencionadas y crear los storage procedures y triggers. Por separado
podemos cargar la tabla ```consumo``` y usar una funcion para aprobar o no esos consumos.

### Tablas que se crearan en la base de datos  

- cliente.
- tarjeta.
- comercio.
- compra.
- rechazo.
- cierre.
- cabecera.
- detalle.
- alerta.
- consumo.

### Llaves primarias y foraneas que se crearan en cada tabla

- Cliente. ```nrocliente``` [PK].
- Tarjeta. ```nrotarjeta``` [PK], ```nrocliente``` [FK].
- Comercio. ```nrocomercio``` [PK].
- Compra. ```nrooperacion``` [PK], ```nrotarjeta```, [FK] ```nrocomercio``` [FK].
- Rechazo. ```nrorechazo``` [PK], ```nrotarjeta``` [FK], ```nrocomercio``` [FK].
- Cierre. ```año``` ```mes``` ```terminacion``` [PK].
- Cabecera. ```nroresumen``` [PK], ```nrotarjeta``` [FK].
- Detalle. ```nroresumen```  ```nrolinea``` [PK], ```nroresumen``` [FK]
- Alerta. ```nroalerta```[PK], ```nrotarjeta``` [FK], ```nrorechazo``` [FK].

### Funciones con las que contamos en la interfaz de usuario

- `crearDataBase()`
  - Crea la base de datos

- `crearTablas()`
  - Crea las tablas necesarias para la correcta administracion de informacion: ```cliente```, ```tarjeta```, ```comercio```, ```compra```, ```rechazo```, ```cierre```, ```cabecera```, ```detalle```, ```alerta```

- `crearPKsFKs()`
  - Asigna a cada tabla creada las llaves primarias y foraneas necesarias con respecto a los campos que poseen y la relaciones entre ellas

- `borrarPKsFKs()`
  - Elimina las llaves primarias y foraneas de las tablas existentes. Esto lo hace en un orden especifico ya que si se borran en cualquier orden perjudica a las tablas que dependen de la llave que pudo haber sido borrada

- `cargarTablas()`
  - Carga las tablas con la informacion tanto de los clientes, de las tarjetas asociadas a cada uno de ellos, comercios, y cierres

- `cargarConsumos()`
  - Carga informacion en la tabla ```consumo```, cada consumo tiene un ```nrotarjeta``` (valido o no), un ```codseguridad``` (codigo de seguridad asociado a la tarjeta, puede ser correcto o incorrecto), ```nrocomercio```(numero de comercio existente o no) y un ```monto```(monto del consumo)

- `crearSPyTriggers()`
  - Crea las funciones en go que crean los Storage Procedures y disparan los triggers necesarios para la correcta administracion de los datos que fueron ingresados, excepto el Storage Procedure `cargarCierresSP()` que fue creado cuando se ejecutó la funcion `cargarTablas()` y `generar_resumenes()` que sera creada y ejecutada en el momento cuando el usuario presione la opcion 9 del menu

- `pasarCosasAcompraORechazo()`
  - Evalua todos los consumos que estan cargados en la tabla ```consumo```, cada uno pasa por revision del SP `autorizar_compra`, si un consumo es invalido no pasa a la tabla compra y pasa a la tabla rechazo a la vez que la se genera un nuevo registro en la tabla ```alerta``` con esos datos.
Si un ```consumo``` es valido, pasa a la tabla ```compra```

- `generarResumenes()`
  - Carga en las tablas cabecera y detalle los datos correspondientes con respecto a las compras realizadas por un cliente en un periodo determinado, estas peticiones de resumenes seran ejecutadas cuando en el momento se cree  el SP `generar_resumenes()` que dentro hace la llamada a `generacion_de_resumen()` de un cliente determinado

### Storage Procedures que se crean en SQL al ejecutar esta funcion. Hay más que se crearon antes o despues, 2 para ser exactos `cargarCierresSP()` y `generar``resumenes()`

- `autorizar_compra` (la crea la funcion `autorizarCompraSP()` de go ).

- `simular_pasar_consumos_a_compra_o_rechazo()` (la crea la funcion `simularPasarConsumosAcompraORechazoSP()` de go)

- `compras_pendientes_de_pago()` (la crea la funcion `comprasPendientesDePagoSP() de go`).

- `generacion_de_resumen()` (la crea la funcion generarResumenesSP() de go)

- `generar_resumenes()` (la crea la funcion generarResumenes() de go tiene muchos `generacion_de_resumen()`).

- `t_a()` (la crea la funcion `alertaClienteTrigger()`)

- `alertas_a_compras()` (la crea la funcion `alertasComprasTrigger()`)

### Triggers que se crean en SQL

- `pasarCosasAcompraORechazo()`
  - Evalua todos los consumos que estan cargados en la tabla consumo, cada uno pasa por revision del SP autorizar``compra, si un consumo es invalido no pasa a la tabla compra y pasa a la tabla rechazo a la vez que la se genera un nuevo registro en la tabla alerta con esos datos. Si un consumo es valido, pasa a la tabla compra.

- `generarResumen()`
  - carga en las tablas cabecera y detalle los datos correspondientes con respecto a las compras realizadas por un cliente en un periodo determinado, estas peticiones de resumenes seran ejecutadas cuando en el momento se cree el SP `generar_resumenes()` que dentro hace la llamada a `generacion_de_resumen()` de un cliente determinado

### Explicacion de SP en ```negocio.sql```

- `cargar_cierres()`
  - Se encarga de ingresar datos a la tabla ```cierre``` dinamicamente iniciando en una fecha

- `autorizar_compra()`
  - Se tiene consumos, cada consumo para pasar a ser una compra debe antes pasar por una autorizacion, esta funcion se encarga de hacer brindar esa autorizacion.
 Toma como parametros dos char's de maximo 16 caracteres, y dos enteros y devuelve un booleano. El primer parametro lo nombramos `i_n_tarjeta`, el segundo `i_cod_seguridad`, el tercero `i_n_comercio` y el cuarto `i_monto`.
 Devuelve true si una tarjeta es valida y false si no lo es.
    - ¿En que se basa para determinar que una tarjeta sea valida?:

    - Una tarjeta es valida cuando el codigo de tarjeta existe en la base de datos, el codigo asociado a esa tarjeta el correcto && cuando el estado de esta tarjeta no es `vencida`, `anulada`, `suspendida`, y el monto total de compras sin pagar + `i_monto` < el limite de compra de la tarjeta (encargado de chequearlo el SP `compras_pendientes_de_pago`). Sacamos el numero de la tarjeta del parametro `i_n_tarjeta`, el codigo de la tarjeta del parametro `i_cod_seguridad` y verificamos si no sobrepasa, sumandole `i_monto` ,al monto total registrado hasta la fecha en compras no pagadas de esa tarjeta.

- `compras_pendientes_de_pago`
  - Es una funcion interna que utiliza `autorizar_compra()`.
Esta funcion toma como parametro un char de 16 caracteres al que denominamos `i_nrotarjeta` y devuelve un entero.
Se encarga de sumar los montos de todas las compras sin abonar que realizo una determinada tarjeta ingresada como parametro.
Esta funcion por si sola no tiene mucho uso, es necesaria para evaluar si el monto total adeudado no excede el limite permitido.

- `simular_pasar_consumos_a_compra_o_rechazo()`
  - Esta funcion no recibe parametros y no devuelve nada.
Se utiliza para recorrer la tabla `consumos` y ver uno por uno si va a parar a la tabla `compra` o a la tabla `rechazo`. ¿Cuales son las condiciones para que un consumo sea una compra y vaya a `compra`?
Dado un consumo (```nrotarjeta```, ```codseguridad```, ```nrocomercio```, ```monto```) se *verifica* que la tarjeta sea valida con ```autorizar```compra()` se le pasa el ```nrotarjeta```, ```codseguridad``` y ```monto``` del consumo, si autorizar compra con esos parametros da true es una compra valida y se completa un registro de la tabla ```compra``` con los datos del consumo i, si da false no lo es y va a parar a la tabla ``rechazo`` de la misma forma ademas de que se generan alerta por fraude que se disparan por triggers.

- `t_a()`
  - Es un SP que se ejecuta cada vez que se ingresa un nuevo registro a la tabla rechazo mediante el trigger `t_a_trigger`, ademas de chequear en el interior de la funcion si la tarjeta que ingresa a rechazo ya posee dos o mas rechazos en el mismo dia Y ademas se genero rechazo por 'limte de compra', si es asi genera una alerta con el codigo de alerta 32 y descripcion de la alerta 'limite', si no es asi el codigo de alerta sera 0 y la descripcion sera 'rechazo'.

- `alertas_a_compras()`
  - Es un SP que alerta cada vez que se genera una nueva compra mediante el trigger `alertas_a_compras_trg`, generara el alerta corespondiente si detecta 2 compras realizadas en menos de 1 min en comercios de la misma localidad o si las compras evaluadas tienen un lapso menor a 5min en locales de distinta localidad

- `generacion_de_resumen`
  - Esta funcion recibe  un numero de cliente y un periodo del año.  
Se encarga de completar los datos en las tablas correspondientes (`cabecera` y `detalle`)teniendo en cuenta todas las compras realizadas por el cliente, dado el ultimo digito de la tarjeta, el año y el mes que recibe la funcion como parametros. Entraran las compras que hizo durante la fecha valida de cierre de la tarjeta.

### Explicacion de Triggers en `negocio.sql`

- `t_a_trigger()`
  - Se ejecuta luego de que se ingrese algun registro nuevo a la tabla `rechazo`, ejecuta el SP `t_a`

- `alertas_a_compras_trg()`
  - Se ejecuta luego de que se ingrese algun registro nuevo a la tabla `compra`, ejecuta el SP `alertas_a_compras`

## Conclusiones

El trabajo practico nos vino muy bien para afianzarnos mucho mas con las bases de datos y poder poner en practica
la teoria aprendida durante toda la materia. Tuvimos que leer documentacion y entender gran parte de los principales metodos
del lenguaje GO y como estos interacturan con PostgresSQL.

Al ser un entorno bastante diferente a la programacion en si y quizas no tener una herramienta que facilite los casos de
test como podria ser JUnit, los metodos de test que nos proponiamos muchas veces eran dificiles de corroborar pero pudimos hacernos
algunos durante todo el trabajo que corrian bien.

Si bien teniamos conocimientos de programacion gracias a las materias anteriores, podemos decir que ahora tenemos el factor
mas importante en un sistema, los datos, sabemos como aplicar teoria y practica para sacar andando un pequeño sistema y que ademas
eficiente y flexible gracias a la utilizacion de Storage Procedures y Triggers.
