iex ((new-object net.webclient).DownloadString('https://chocolatey.org/install.ps1'))
$env:PATH = "$env:PATH;$env:ALLUSERSPROFILE"