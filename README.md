
etlite
====

Etl es la sigla de Extraction-Transformation-and-Load. Este software tiene como objetivo el procesar archivos de texto con formato fijo y actualizar una tabla en la base de datos de la forma más liviana posible, basado en un "mapa" con un origen y un destino.

Ejemplo de mapa:

<?xml version="1.0" encoding="UTF-8"?>
<!-- Tag base -->
<map>
	<!-- Se requiere UN source, el cual determina los tipos de campo que se soportan en fields/field/source/ -->
	<source>
		<!-- 
		Para cualquier source es obligatorio configurar un path, el cual puede ser una ruta estática o una variable pasada por línea de comando.
		Cuando el path comienza con "$", el etl espera un parámetro con el nombre a continuación de tal símbolo, por ejemplo, el path "$foobar" indica que en la línea de comando se debe pasar como argumento el parámetro "-foobar /ruta/al/archivo"
		-->
		
		<!-- Si el source es un fixed-file-format o fff, el tipo de campo soportado es "fixed". Este tipo de source se usa para archivos de ancho fijo -->
		<fff_document path="$input_file"/>

		<!-- Si el source es un cardfile_document, el tipo de campo soportado es "floating_field". Este source es útil cuando la entrada es una "ficha", con un encabezado de varias líneas y diferentes campos por cada línea, con una sección de detalle con columnas de ancho fijo -->
		<fff_document path="$input_file"/>
	</source>

	<!-- El target sólo puede ser "database". En éste se configura la conexión, la tabla de destino y la llave por la que se actualiza -->
	<target>
		<database>
			<host>localhost</host>
			<user>itgr_user</user>
			<pass>itgruser0112</pass>
			<dbname>itgr</dbname>
			<default_table>cruce</default_table>
			<keys>sc_id</keys>
		</database>
	</target>
	
	<!-- La sección de fields es el mapa que define qué parte del source se debe guardar en cuál campo de la tabla -->
	<fields>
		<field>
			<source>
				<!-- Para el source fff, el campo debe ser fixed. En éste se define el caracter de inicio y fin -->
				<fixed from="3" to="21"/>

				<!-- Para el card file, el campo debe ser floating_field. En éste se define las líneas y el caracter de inicio y fin. Incorpora también el type que puede ser header y row (por defecto), el primero provoca que el campo se lea la primera vez y luego se retorne siempre el mismo valor, a pesar de que se halle otros datos válidos en líneas subsiguientes, el segundo tipo no mantiene el dato entre líneas -->
				<floating_field line_from="1" line_to="1" col_from="23" col_to="45" type="header|row"/>

				<!-- Para cualquier source, se puede validar usando una expresión regular con la función expect, si el dato hallado no cumple la condición, el procesamiento se detiene a menos que "required" sea "no" -->
				<expects>^\d\d:\d\d:\d\d \d\d-\w\w\w-\d\d$</expects>

				<!-- Con el modo "hold" el valor se mantiene hasta que se halla otro válido -->
				<mode>hold</mode>

				<!-- El campo required indica si un dato no válido detiene o no el procesamiento -->
				<required>no</required>
			</source>
			<target>
				<!-- El campo target siempre es column. Opcionalmente puede llevar el atributo date_format, con lo cual el dato es tratado como fecha -->
				<column date_format="%H:%i:%S %d-%b-%y">actualizado</column>
			</target>
		</field>
		
		<field>
			<source>
				<fixed from="6" to="13"/>
				<expects>^J[\d*]</expects>
			</source>
			<target>
				<column>sc_id</column>
			</target>
		</field>

		<field>
			<source>
				<fixed from="30"/>
				<expects>^is[\s*]</expects>
			</source>

			<!--
			Para cualquier campo, se pueden usar 6 transformaciones, las cuales modifican el valor obtenido desde el source antes de pasarlo a la columna.
			-->
			<transformations>
				<!-- Trim: Quita espacios en blanco de la derecha (right), la izquierda (left) o ambos lados (both) -->
				<trim side="both"/>

				<!-- Dictionary: reemplaza todo el campo (all) o sólo la parte mapeada (inline) a partir de un mapa (map_from -> map_to) creado desde una consulta en la base de datos -->
				<dictionary replace="all">
					<database>
						<host>localhost</host>
						<user>itgr_user</user>
						<pass>itgruser0112</pass>
						<dbname>itgr</dbname>
						<sql_query>SELECT meta_description AS map_from, id AS map_to FROM status</sql_query>
					</database>
				</dictionary>

				<!-- Save_to: almacena temporalmente el valor del campo en una variable global -->
				<save_to>variable</save_to>

				<!-- Join_with: prefija el valor de una variable de ambiente con el valor del campo -->
				<join_with>variable</join_with>

				<!-- Wrap: genera una expresión cualquiera en la que se reemplaza cada %VALUE% por el valor del campo  -->
				<wrap>expresion con %VALUE% parar reemplazar</wrap>

				<!-- Date_sufix: prefija la fecha en el formato especificado al valor del campo  -->
				<date_sufix format=" %d-%m-%Y"/>

			</transformations>
			<target>
				<column>status_id</column>
			</target>
		</field>

	</fields>
</map>
