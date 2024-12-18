export namespace main {
	
	export class ExtractionMethod {
	    Name: string;
	    Schema: string;
	
	    static createFrom(source: any = {}) {
	        return new ExtractionMethod(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.Name = source["Name"];
	        this.Schema = source["Schema"];
	    }
	}

}

