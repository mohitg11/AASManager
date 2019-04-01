function logMessage {
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $true, Position = 0)] [string] $LogMessage
    )

    Write-Output ("{0} - {1}" -f $(Get-Date -Format u), $LogMessage)
}


function getAutomationVariable {
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $true)] [string] $Name
    )

    switch ($Name) {
        "server" { $result = Get-AutomationVariable -Name 'defaultAASServer' }
        "refresh" { $result = Get-AutomationVariable -Name 'defaultRefreshType' }
        "tenant" { $result = Get-AutomationVariable -Name 'defaultTenantId' }
        "location" { $result = Get-AutomationVariable -Name 'defaultAASLocation' }
        "cred" { $result = Get-AutomationVariable -Name 'defaultCredentials' }
    }

    return $result

}


function Set-DefaultAASConnection {
    <#
    .DESCRIPTION
        Set default connection paramaters for the session

        The function uses the following automation variables for default values if not provided:
            - defaultAASLocation    - Azure analysis services instance location,
                                    eg northeurope.asazure.windows.net
            - defaultCredentials    - Name of credentials to use, these must be defined in the automation
                                    resource as well
            - defaultTenantId       - Azure Tenant ID

    .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable, eg northeurope
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $false)] [string] $tenant = $(getAutomationVariable -Name 'tenant'),
        [Parameter(Mandatory = $false)] [string] $cred = $(getAutomationVariable -Name 'cred'),
        [Parameter(Mandatory = $false)] [string] $location = $(getAutomationVariable -Name 'location')
    )

    $Script:defaultConnection = "" | Select-Object -Property tenant, cred, location
    $Script:defaultConnection.tenant = $tenant
    $Script:defaultConnection.cred = $cred
    $Script:defaultConnection.location = $location

}


function Get-DefaultAASConnection {
    <#
    .DESCRIPTION
        The function uses the following automation variables for default values if not set explicity:
            - defaultAASLocation    - Azure analysis services instance location,
                                    eg northeurope.asazure.windows.net
            - defaultCredentials    - Name of credentials to use, these must be defined in the automation
                                    resource as well
            - defaultTenantId       - Azure Tenant ID

        Use Set-DefaultAASConnection to set the default values for the session
    #>

    [CmdletBinding()]
    Param ()

    if (!$Script:defaultConnection) {
        Set-DefaultAASConnection
    }

    return $Script:defaultConnection
}


function Set-DefaultAASServer {
    <#
    .DESCRIPTION
        Set default server for the session

        The function uses the following automation variables for default values if not provided:
            - defaultAASServer      - Azure analysis services instance name

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(getAutomationVariable -Name 'server')
    )

    $Script:defaultServer = $server
}


function Get-DefaultAASServer {
    <#
    .DESCRIPTION
        Get default server for the session

        Use Set-DefaultAASServer to set the default values for the session
    #>

    [CmdletBinding()]
    Param ()

    if (!$Script:defaultServer) {
        Set-DefaultAASServer
    }

    return $Script:defaultServer
}


function Set-DefaultAASRefreshType {
    <#
    .DESCRIPTION
        Set default refresh type for the session

        The function uses the following automation variables for default values if not provided:
            - defaultRefreshType    - Default Refresh type, eg, full, automatic, clearvalues

    .PARAMETER refresh
        Process mode to use - Optional, defaults to defaultRefreshType automation variable,
        eg, full, automatic, clearvalues
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $false)] [string] $refresh = $(getAutomationVariable -Name 'refresh')
    )

    $Script:defaultRefresh = $refresh
}


function Get-DefaultAASRefreshType {
    <#
    .DESCRIPTION
        Get default refresh type for the session

        Use Set-DefaultAASRefreshType to set the default values for the session
    #>

    [CmdletBinding()]
    Param ()

    if (!$Script:defaultRefresh) {
        Set-DefaultAASRefreshType
    }

    return $Script:defaultRefresh
}


function Connect-AAS {
    <#
    .DESCRIPTION
        Connects to your tenant using the provided details or session defaults.
        Check Get-DefaultAASConnection help for more details

    .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>

    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $false)] [string] $tenant,
        [Parameter(Mandatory = $false)] [string] $cred,
        [Parameter(Mandatory = $false)] [string] $location
    )

    if ([String]::IsNullOrWhiteSpace($tenant)) { $tenant = $($(Get-DefaultAASConnection).tenant) }
    if ([String]::IsNullOrWhiteSpace($cred)) { $cred = $($(Get-DefaultAASConnection).cred) }
    if ([String]::IsNullOrWhiteSpace($location)) { $location = $($(Get-DefaultAASConnection).location) }

    try {
        $credential = Get-AutomationPSCredential -Name $cred -ErrorAction Stop
    }
    catch {
        Write-Error "Unable to get credentials"
        Write-Error $_
    }


    # Log in to Azure Analysis Services using the Azure AD Service Principal
    logMessage ('Authenticating to {0}' -f $location)
    $params = @{
        Credential         = $credential
        ServicePrincipal   = $true
        TenantId           = $tenant
        RolloutEnvironment = $location
    }
    try {
        Add-AzureAnalysisServicesAccount @params -ErrorAction Stop
        logMessage ('Authentication to {0} complete' -f $location)
    }
    catch {
        Write-Error "Authentication Failed"
        Write-Error $_
    }
}


function connectAASAuto {
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $false)] [alias("c")] [switch] $connect,
        [Parameter(Mandatory = $false)] [string] $tenant,
        [Parameter(Mandatory = $false)] [string] $cred,
        [Parameter(Mandatory = $false)] [string] $location
    )

    if ($connect.IsPresent) {
        Connect-AAS -tenant $tenant -cred $cred -location $location
    }

}


function Invoke-AASQuery {
    <#
    .DESCRIPTION
        Run an TMSL query on your AAS server.

    .PARAMETER query
        TMSL Query to run - Mandatory

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>

    [CmdletBinding(DefaultParameterSetName = 'Standard')]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)]  [string] $query,

        [Parameter(ParameterSetName = 'Connect', Mandatory = $true)]  [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $tenant,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $cred,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    # Run an TMSL query
    logMessage "Executing TMSL query"
    try {
        Invoke-ASCmd -Server $server -Query $query -ErrorAction Stop
        logMessage "Query Execution Complete"
    }
    catch {
        Write-Error "Query Execution Failed"
        Write-Error $_
    }
}


function Invoke-AASRefresh {
    <#
    .DESCRIPTION
        Process a tabular model in Azure Analysis Services. You can process the complete database,
        a table, or a partition.
        The script will search and process in the order given below:
            1. Partition    - Partition, table and database need to be passed
            2. Table        - Table and database need to be passed
            3. Database     - Database needs to be passed

    .PARAMETER database
        Database to process or connect to - Mandatory

    .PARAMETER table
        Table to process - Optional

    .PARAMETER partition
        Partititon to process - Optional

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER refresh
        Process mode to use - Optional, defaults to defaultRefreshType automation variable or session default,
        eg, full, automatic, clearvalues

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>
    [CmdletBinding(DefaultParameterSetName = 'Database')]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $false)] [string] $refresh = $(Get-DefaultAASRefreshType),

        [Parameter(ParameterSetName = 'Table', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Table w/ Connect', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Partition', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Partition w/ Connect', Mandatory = $true)] [string] $table,
        [Parameter(ParameterSetName = 'Partition', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Partition w/ Connect', Mandatory = $true)] [string] $partition,

        [Parameter(ParameterSetName = 'Database w/ Connect', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Table w/ Connect', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Partition w/ Connect', Mandatory = $true)] [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Database w/ Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Table w/ Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Partition w/ Connect', Mandatory = $false)][string] $tenant,
        [Parameter(ParameterSetName = 'Database w/ Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Table w/ Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Partition w/ Connect', Mandatory = $false)][string] $cred,
        [Parameter(ParameterSetName = 'Database w/ Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Table w/ Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Partition w/ Connect', Mandatory = $false)][string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    $params = @{
        server      = $server
        RefreshType = $refresh
    }

    if ($table) {
        if ($partition) {
            # Perform a refresh of the partition
            $params += @{ Database = $database ; TableName = $table; PartitionName = $partition }
            logMessage "Processing ($refresh) $partition partition in $table table in $database database"

            try {
                Invoke-ProcessPartition @params -ErrorAction Stop
                logMessage "$partition partition in $table table in $database database processesed"
            }
            catch {
                Write-Error "Processing $partition partition in $table table in $database database failed"
                Write-Error $_
            }

        }

        else {
            # Perform a refresh of the table
            $params += @{ DatabaseName = $database ; TableName = $table }
            logMessage "Processing ($refresh) $table table in $database"

            try {
                Invoke-ProcessTable @params -ErrorAction Stop
                logMessage "$table table in $database database processesed"
            }
            catch {
                Write-Error "Processing $table table in $database database failed"
                Write-Error $_
            }

        }
    }

    else {
        # Perform a refresh of the database
        logMessage "Processing ($refresh) $database"

        try {
            Invoke-ProcessASDatabase -DatabaseName $database @params -ErrorAction Stop
            logMessage "$database database processesed"
        }
        catch {
            Write-Error "Processing $database database failed"
            Write-Error $_
        }

    }

}


function Get-AASCreatePartition {
    <#
    .DESCRIPTION
        Generate a TMSL query to create a partiton in a specified database and table using the provided
        sql query and datasource

    .PARAMETER datasource
        Datasource to use for partition - Mandatory

    .PARAMETER sql
        Partition SQL Query - Mandatory

    .PARAMETER database
        Name of database where table- Mandatory

    .PARAMETER table
        Name of table to partition - Mandatory

    .PARAMETER partition
        Name of partition to create - Mandatory
    #>
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $true)]  [string] $datasource,
        [Parameter(Mandatory = $true)]  [string] $sql,
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $true)]  [string] $table,
        [Parameter(Mandatory = $true)]  [string] $partition
    )

    # Build TMSL Query
    $tmslQuery = "
        {{
            `"createOrReplace`": {{
                `"object`": {{
                    `"database`": `"{0}`",
                    `"table`": `"{1}`",
                    `"partition`": `"{2}`"
                }},
                `"partition`": {{
                    `"name`": `"{2}`",
                    `"source`": {{
                        `"query`": `"{3}`",
                        `"dataSource`": `"{4}`"
                    }}
                }}
            }}
        }}" -f $database, $table, $partition, $sql, $datasource

    return $tmslQuery;
}


function Invoke-AASCreatePartition {
    <#
    .DESCRIPTION
        Create a partition in a table in a given database using the query and datasource provided.

    .PARAMETER datasource
        Datasource to use for partition - Mandatory

    .PARAMETER sql
        Partition SQL Query - Mandatory

    .PARAMETER database
        Name of database where table- Mandatory

    .PARAMETER table
        Name of table to partition - Mandatory

    .PARAMETER partition
        Name of partition to create - Mandatory

    .PARAMETER proccess
        Switch for processing after creation, alias p - Optional

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER refresh
        Process mode to use - Optional, defaults to defaultRefreshType automation variable or session default,
        eg, full, automatic, clearvalues

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>

    [CmdletBinding(DefaultParameterSetName = 'Standard')]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $true)]  [string] $table,
        [Parameter(Mandatory = $true)]  [string] $partition,
        [Parameter(Mandatory = $true)]  [string] $sql,
        [Parameter(Mandatory = $true)]  [string] $datasource,

        [Parameter(Mandatory = $false)] [alias("p")] [switch] $process,
        [Parameter(Mandatory = $false)] [string] $refresh = $(Get-DefaultAASRefreshType),

        [Parameter(ParameterSetName = 'Connect', Mandatory = $true)]  [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $tenant,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $cred,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    $params = @{
        database  = $database
        table     = $table
        partition = $partition
    }

    $tmslQuery = Get-AASCreatePartition @params -datasource $datasource -sql $sql

    # Execute TMSL query
    logMessage "(Re)Creating $partition partition on $table table on $database database from $datasource datasource"
    Write-Verbose $tmslQuery
    try {
        Invoke-AASQuery -query $tmslQuery -server $server -ErrorAction Stop
        logMessage "Partition $partition successfully (re)created"

        if ($process.IsPresent) {
            Invoke-AASRefresh @params -server $server -refresh $refresh
        }
    }
    catch {
        Write-Error "Partition $partition creation failed"
        Write-Error $_
    }
}


function Get-AASDeletePartition {
    <#
    .DESCRIPTION
        Generate a TMSL query to delete a partiton in a specified database and table

    .PARAMETER database
        Name of database where table- Mandatory

    .PARAMETER table
        Name of table to partition - Mandatory

    .PARAMETER partition
        Name of partition to create - Mandatory
    #>
    [CmdletBinding()]
    Param (
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $true)]  [string] $table,
        [Parameter(Mandatory = $true)]  [string] $partition
    )

    # Build TMSL Query
    $tmslQuery = "
        {{
            `"delete`": {{
                `"object`": {{
                    `"database`": `"{0}`",
                    `"table`": `"{1}`",
                    `"partition`": `"{2}`"
                }}
            }}
        }}" -f $database, $table, $partition

    return $tmslQuery

}


function Invoke-AASDeletePartition {
    <#
    .DESCRIPTION
        Delete a partition in a table in a given database.

    .PARAMETER database
        Name of database where table- Mandatory

    .PARAMETER table
        Name of table to partition - Mandatory

    .PARAMETER partition
        Name of partition to create - Mandatory

    .PARAMETER safe
        Switch for safe deletion, if partition doesn't exist, it will be created and then deleted,
        alias c - Optional

    .PARAMETER datasource
        Datasource to use for partition creation if using safe delete - Optional

    .PARAMETER sql
        Partition SQL Query to use for partition creation if using safe delete - Optional

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>

    [CmdletBinding(DefaultParameterSetName = 'Standard')]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $true)]  [string] $table,
        [Parameter(Mandatory = $true)]  [string] $partition,

        [Parameter(ParameterSetName = 'Safe Delete', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $true)]  [alias("s")] [switch] $safe,
        [Parameter(ParameterSetName = 'Safe Delete', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $true)]  [string] $datasource,
        [Parameter(ParameterSetName = 'Safe Delete', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $true)]  [string] $sql,

        [Parameter(ParameterSetName = 'Connect', Mandatory = $true)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $true)]  [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $false)] [string] $tenant,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $false)] [string] $cred,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)]
        [Parameter(ParameterSetName = 'Safe Delete w/ Connect', Mandatory = $false)] [string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    $params = @{
        database  = $database
        table     = $table
        partition = $partition
    }

    if ($safe.IsPresent) {
        logMessage "Safe delete is on, (re)creating partition"
        $paramsCreate = @{
            server     = $server
            sql        = $sql
            datasource = $datasource
        }
        try {
            Invoke-AASCreatePartition @params @paramsCreate -ErrorAction Stop
        }
        catch {
            Write-Error "Partition creation failed"
            Write-Error $_
        }
    }

    # Build TMSL Query
    $tmslQuery = Get-AASDeletePartition @params
    # Execute TMSL query
    logMessage "Deleting $partition partition on $table table on $database database"
    Write-Verbose $tmslQuery
    try {
        Invoke-AASQuery -query $tmslQuery -server $server -ErrorAction Stop
        logMessage "Partition $partition deleted"
    }
    catch {
        Write-Error "Failed to delete $partition partition"
        Write-Error $_
    }

}


function getPartitionDetails {
    [CmdletBinding()]
    [OutputType([System.Object[]])]
    Param (
        [Parameter(Mandatory = $true)] [string] $partition,
        [Parameter(Mandatory = $true)] [string] $sql,
        [Parameter(Mandatory = $true)] [string] $replace,
        [Parameter(Mandatory = $true)] [string] $dateColumn,
        [Parameter(Mandatory = $true)] [datetime] $startDate,
        [Parameter(Mandatory = $true)] [datetime] $endDate,
        [Parameter(Mandatory = $false)][string] $dateFormatName = "MMM-yyyy"
    )

    $dateFormatQuery = "yyyyMMdd"

    $params = @(
        $dateColumn
        $startDate.ToString($dateFormatQuery)
        $endDate.ToString($dateFormatQuery)
    )

    $timeCondition = "{0} >= '{1}' AND {0} < '{2}'" -f $params
    $sql = $sql.Replace($replace, $timeCondition)
    $monthPartitionName = "{0}{1}" -f $partition, $startDate.ToString($dateFormatName)

    return $monthPartitionName, $sql
}


function Invoke-AASCreateTimeBasedPartition {
    <#
    .DESCRIPTION
        Create a partition in a table between two dates, using a sql query template. The template must have a
        replaceable string which wil be replaced by the between two dates condition.
        Eg below parameters will generate the query,
        "SELECT * FROM someTable WHERE Date >= '20180101' AND Date < '20180201'",
        with the partition name being "FactTable_Jan-2018"
            - sql           = "SELECT * FROM someTable WHERE {0}"
            - dateColumn    = "Date"
            - startDate     = GetDate -Day 1 -Month 1 -Year 2018
            - endDate       = GetDate -Day 1 -Month 2 -Year 2018
            - Partition     = "FactTable_"
            - dateFormatName = "MMM-yyyy"

    .PARAMETER datasource
        Datasource to use for partition - Mandatory

    .PARAMETER database
        Database to connect to - Mandatory

    .PARAMETER table
        Table which will be paritioned - Mandatory

    .PARAMETER sql
        Partition SQL query template to use - Mandatory

    .PARAMETER partition
        Partititon name prefix - Optional

    .PARAMETER replace
        Search string to search in template which will be replaced with the time condition.
        Default is {0}. Optional

    .PARAMETER dateColumn
        Date column used for time condition - Mandatory

    .PARAMETER startDate
        Starting date of the partition. This date is included as part of the partition - Mandatory

    .PARAMETER endDate
        Ending date of the partition. This date is not included as part of the partition - Mandatory

    .PARAMETER dateFormatName
        Format of the date to be used in the parition name. For valid formats, check
        https://docs.microsoft.com/en-us/dotnet/standard/base-types/custom-date-and-time-format-strings - Mandatory

    .PARAMETER proccess
        Switch for processing after creation, alias p - Optional

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER refresh
        Process mode to use - Optional, defaults to defaultRefreshType automation variable or session default,
        eg, full, automatic, clearvalues

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>
    [CmdletBinding(DefaultParameterSetName = 'Standard')]
    Param (
        [Parameter(Mandatory = $true)] [string] $partition,
        [Parameter(Mandatory = $true)] [string] $sql,
        [Parameter(Mandatory = $false)] [string] $replace = "{0}",
        [Parameter(Mandatory = $true)] [string] $dateColumn,
        [Parameter(Mandatory = $true)] [datetime] $startDate,
        [Parameter(Mandatory = $true)] [datetime] $endDate,
        [Parameter(Mandatory = $true)] [string] $dateFormatName,

        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)] [string] $database,
        [Parameter(Mandatory = $true)] [string] $table,
        [Parameter(Mandatory = $true)] [string] $datasource,

        [Parameter(Mandatory = $false)] [alias("p")] [switch] $process,
        [Parameter(Mandatory = $false)] [string] $refresh = $(Get-DefaultAASRefreshType),

        [Parameter(ParameterSetName = 'Connect', Mandatory = $true)]  [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $tenant,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $cred,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    $params = @{
        partition      = $partition
        sql            = $sql
        replace        = $replace
        dateColumn     = $dateColumn
        startDate      = $startDate
        endDate        = $endDate
        dateFormatName = $dateFormatName
    }
    $partitionName, $sql = getPartitionDetails @params

    $paramsCreate = @{
        server     = $server
        database   = $database
        table      = $table
        datasource = $datasource
        partition  = $partitionName
        sql        = $sql
        process    = $process
        refresh    = $refresh
    }
    Invoke-AASCreatePartition @paramsCreate
}


function Invoke-AASManageYearPartition {
    <#
    .DESCRIPTION
        Automatically manage the creation and processing of yearly partitions of a table based on the sql query
        and date column specified. The previous year partition is processed for the first 7 days of a new year to
        enseure all data has been processed.
        Eg below parameters for a date in 2019 will generate the query,
        "SELECT * FROM someTable WHERE Date >= '20190101' AND Date < '20200101'",
        with the partition name being "FactTable_2019"
            - sql           = "SELECT * FROM someTable WHERE {0}"
            - dateColumn    = "Date"
            - Partition     = "FactTable_"

    .PARAMETER datasource
        Datasource to use for partition - Mandatory

    .PARAMETER database
        Database to connect to - Mandatory

    .PARAMETER table
        Table which will be paritioned - Mandatory

    .PARAMETER sql
        Partition SQL query template to use - Mandatory

    .PARAMETER partition
        Partititon name prefix - Optional

    .PARAMETER replace
        Search string to search in template which will be replaced with the time condition.
        Default is {0}. Optional

    .PARAMETER dateColumn
        Date column used for time condition - Mandatory

    .PARAMETER justCreate
        Switch for disabling the processing of the parition. This is useful for debuging, alias jc - Optional

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER refresh
        Process mode to use - Optional, defaults to defaultRefreshType automation variable or session default,
        eg, full, automatic, clearvalues

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>
    [CmdletBinding(DefaultParameterSetName = 'Standard')]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $true)]  [string] $table,
        [Parameter(Mandatory = $true)]  [string] $datasource,

        [Parameter(Mandatory = $true)]  [string] $partition,
        [Parameter(Mandatory = $true)]  [string] $sql,
        [Parameter(Mandatory = $false)] [string] $replace = "{0}",
        [Parameter(Mandatory = $true)]  [string] $dateColumn,

        [Parameter(Mandatory = $false)] [alias("jc")] [switch] $justCreate,
        [Parameter(Mandatory = $false)] [string] $refresh = $(Get-DefaultAASRefreshType),

        [Parameter(ParameterSetName = 'Connect', Mandatory = $true)]  [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $tenant,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $cred,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    # Get the current day, month and year
    $currentDate = Get-Date
    $currentMonth = $currentDate.Month
    $currentDay = $currentDate.Day
    $dateFormatName = "yyyy"

    $params = @{
        partition      = $partition
        sql            = $sql
        replace        = $replace
        dateColumn     = $dateColumn
        server         = $server
        database       = $database
        table          = $table
        datasource     = $datasource
        process        = (-not $justCreate)
        refresh        = $refresh
        dateFormatName = $dateFormatName
        startDate      = (Get-Date -Day 01 -Month 01)
        endDate        = (Get-Date -Day 01 -Month 01).AddYears(1)
    }

    # (Re)Create current year's partition and process
    Invoke-AASCreateTimeBasedPartition @params

    # Check for first 7 days of the year
    if ($currentMonth -eq 1 -and $currentDay -le 7) {

        # (Re)Create last year's partition and process
        $params.endDate = $params.startDate
        $params.startDate = $params.endDate.AddYears(-1)
        Invoke-AASCreateTimeBasedPartition @params

    }

}


function Invoke-AASManageYearMonthPartition {
    <#
    .DESCRIPTION
        Automatically manage the creation and processing of yearly and monthly partitions of a table based on the
        sql query and date column specified. At the start of a new month a new month parition is created and the
        previous month parition is processed on the first day of the new month. At the start of a new year, all
        previous year's month paritions are consolidated and reprocessed for the previous year.
        Eg below parameters for a date in 2019 will generate the query,
        "SELECT * FROM someTable WHERE Date >= '20190101' AND Date < '20200101'",
        with the partition name being "FactTable_2019"
            - sql           = "SELECT * FROM someTable WHERE {0}"
            - dateColumn    = "Date"
            - Partition     = "FactTable_"

    .PARAMETER datasource
        Datasource to use for partition - Mandatory

    .PARAMETER database
        Database to connect to - Mandatory

    .PARAMETER table
        Table which will be paritioned - Mandatory

    .PARAMETER sql
        Partition SQL query template to use - Mandatory

    .PARAMETER partition
        Partititon name prefix - Optional

    .PARAMETER replace
        Search string to search in template which will be replaced with the time condition.
        Default is {0}. Optional

    .PARAMETER dateColumn
        Date column used for time condition - Mandatory

    .PARAMETER justCreate
        Switch for disabling the processing of the parition. This is useful for debuging, alias jc - Optional

    .PARAMETER server
        URL of AAS server - Optional, defaults to defaultAASServer automation variable or session default

    .PARAMETER refresh
        Process mode to use - Optional, defaults to defaultRefreshType automation variable or session default,
        eg, full, automatic, clearvalues

    .PARAMETER connect
        Switch for connecting to AAS using default or provied parameters if not already connected,
        check Get-DefaultAASConnection for more details on paramaters, alias c - Optional

     .PARAMETER tenant
        Azure Tenant ID - Optional, defaults to defaultTenantID automation or session default

    .PARAMETER cred
        Azure Automation credentials to use - Optional, defaults to defaultCredentials automation variable
         or session default

    .PARAMETER location
        Location of AAS server - Optional, defaults to defaultAASLocation automation variable
         or session default, eg northeurope
    #>
    [CmdletBinding(DefaultParameterSetName = 'Standard')]
    Param (
        [Parameter(Mandatory = $false)] [string] $server = $(Get-DefaultAASServer),
        [Parameter(Mandatory = $true)]  [string] $database,
        [Parameter(Mandatory = $true)]  [string] $table,
        [Parameter(Mandatory = $true)]  [string] $datasource,

        [Parameter(Mandatory = $true)]  [string] $partition,
        [Parameter(Mandatory = $true)]  [string] $sql,
        [Parameter(Mandatory = $false)] [string] $replace = "{0}",
        [Parameter(Mandatory = $true)]  [string] $dateColumn,

        [Parameter(Mandatory = $false)] [alias("jc")] [switch] $justCreate,
        [Parameter(Mandatory = $false)] [string] $refresh = $(Get-DefaultAASRefreshType),

        [Parameter(ParameterSetName = 'Connect', Mandatory = $true)]  [alias("c")] [switch] $connect,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $tenant,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $cred,
        [Parameter(ParameterSetName = 'Connect', Mandatory = $false)] [string] $location
    )

    connectAASAuto -tenant $tenant -cred $cred -location $location -connect:$connect

    # Get the current day, month and year
    $currentDate = Get-Date
    $currentYear = $currentDate.Year
    $currentMonth = $currentDate.Month
    $currentDay = $currentDate.Day
    $dateFormatName = "MMM-yyyy"

    $params = @{
        partition  = $partition
        sql        = $sql
        replace    = $replace
        dateColumn = $dateColumn
    }
    $paramsCreate = @{
        server     = $server
        database   = $database
        table      = $table
        datasource = $datasource
    }
    $paramsProcess = @{
        process = (-not $justCreate)
        refresh = $refresh
    }
    $paramsD = @{
        dateFormatName = $dateFormatName
        startDate      = (Get-Date -Day 01 -Month $currentMonth)
        endDate        = (Get-Date -Day 01 -Month $currentMonth).AddMonths(1)
    }

    # (Re)Create current month's partition and process
    Invoke-AASCreateTimeBasedPartition @params @paramsCreate @paramsProcess @paramsD

    # Check for 1st of Month
    if ($currentDay -eq 1) {

        #Check for January
        if ($currentMonth -eq 1) {

            # (Re)Create last year's partition and full process if 01 Jan
            $paramsY = @{ dateFormatName = "yyyy" }
            $paramsY.Add('endDate', (Get-Date -Day 01 -Month 01 -Year $currentYear))
            $paramsY.Add('startDate', $paramsY.endDate.AddYears(-1))
            $paramsProcess.refresh = "Full"
            Invoke-AASCreateTimeBasedPartition @params @paramsCreate @paramsProcess @paramsY

            # (safe) Delete all month partitions from last year
            for ($month = 1; $month -le 12; $month++) {
                $paramsD.startDate = (Get-Date -Day 01 -Month $month -Year ($currentYear - 1))
                $paramsD.endDate = $paramsD.startDate.AddMonths(1)
                $monthPartitionName , $sql = getPartitionDetails @params @paramsD
                Invoke-AASDeletePartition @paramsCreate -partition $monthPartitionName -safe -sql $sql
            }

        }

        # (Re)Create last month's partition and process if 1st of the month
        else {

            $paramsD.startDate = ((Get-Date -Day 01 -Month $currentMonth).AddMonths(-1))
            $paramsD.endDate = $paramsD.startDate.AddMonths(1)
            Invoke-AASCreateTimeBasedPartition @params @paramsCreate @paramsProcess @paramsD

        }

    }

}