// in progress
#ParamGen

ParamGen is a conduit tool that generates the parameters map from a certain Go struct, and this map should be returned 
by the `Configure` method in the connector.

##Installation
Once you have installed Go, install the paramgen tool.
````
go install github.com/ConduitIO/conduit-connector-sdk/cmd/paramgen@v0.4.0
````


###Parameter Tags 
In order to give your parameter a name, a default value, or add some validations to it, tags are the way to go.
We have three tags that can be parsed:
1. `json`: this tag is used to rename the parameter.
   ```go
   Name string `json:"first-name"`
   
2. `default`: sets the default value for the parameter.
      ```go
   Name string `default:"conduit"`
3. `validate`: adds builtin validations to the parameter, these validations will be executed by conduit once a connector
   is configured. Use comma to add multiple validations. 
   We have 5 main validations:
    * `required`: a boolean tag to indicate if a field is required or not. If it was added to the validate tag without a
    value, then we assume the field is required.
      ```go
      NameRequired    string `validate:"required"`
      NameRequired2   string `validate:"required=true"`
      NameNotRequired string `validate:"required=false"`
    * `lt` or `less-than`: takes an int or a float value, indicated that the parameter should be less than the value provided.
    * `gt` or `greater-than`: takes an int or a float value, indicated that the parameter should be greater than the value provided.

      ```go
      Age int `validate:"gt=0,lt=200"`
      Age2 float `validate:"greater-than=0,less-than=200.2"`
    * `inclusion`: validates that the parameter value is included in a specified list, this list values are separated
      using a pipe character `|`.
      ```go
      Gender string `validate:"inclusion=male|female|other"`
   * `exclusion`: validates that the parameter value is NOT included in a specified list, this list values are separated
      using a pipe character `|`.
      ```go
      Color string `validate:"exclusion=red|green"`