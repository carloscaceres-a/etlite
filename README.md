#etlite

Etl stands for Extraction-Transformation-and-Load. This software has as purpose to process fixed format text files and update a database table as lightweight and fast as possible, based on a simple "map" with a source, a target and transformations between both.

Example:

```
<?xml version="1.0" encoding="UTF-8"?>
<!-- Tag base -->
<map>
    <!-- ONE source required, this defines what field kinds are supported in fields/field/source/-->
	<source>
		<!-- For any source is required set a path wich can be a static path or a command line passed variable. When the path starts with "$", etlite expects a param named like that path (without the dollar sign); for example, a path "$foobar" says that command line invocation must pass "-foobar /path/to/file"
		-->
		
		<!-- If source is a fixed-file-format or fff, the field type supported is "fixed". This kind of source is useful for well known width files -->
		<fff_document path="$input_file"/>

		<!-- If source is a cardfile_document, the field type supported is "floating_field". This source is useful when input is like a card, with a several lines header and different fields for each line, and a detail section with fixed width columns -->
		<cardfile_document path="$input_file"/>
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
```