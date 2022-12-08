# oc_9_azfunc

This is the azure functions developed for recommandation systems.

Here is the architecture used for the application.
![oc9](https://user-images.githubusercontent.com/16722943/206492353-83b3857e-9a16-407f-8692-d1c73412e809.png)

 - First we create the azure functions in the azure portal. 
 - Next we use the Visual Studio Code and the Azure plugin to develop and deploy the azure functions. 
 - We use the Azure Blob Storage to store our datasets. 
 - The user will enter the application and will make request to the azure function that will triger data and will make 5 article recommandations for the users.
