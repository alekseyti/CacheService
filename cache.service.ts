import { Injectable } from '@angular/core';
import { Observable, Subject, of, throwError } from "rxjs";
import { tap, catchError } from 'rxjs/operators';
interface CacheContent {
  expiry: number;
  value: any;
}


@Injectable({
  providedIn: 'root'
})
export class CacheService {

  private cache: Map<string, CacheContent> = new Map<string, CacheContent>();
  private inFlightObservables: Map<string, Subject<any>> = new Map<string, Subject<any>>();
  readonly DEFAULT_MAX_AGE: number = 300000;

  constructor() { }


  get(key: string, fallback?: Observable<any>, maxAge?: number): Observable<any> | Subject<any> {
    if (this.hasValidCachedValue(key)) {
      console.log(`%cGetting from cache ${key}`, 'color: green');
      return of(this.cache.get(key).value);
    }

    if (!maxAge) {
      maxAge = this.DEFAULT_MAX_AGE;
    }

    if (this.inFlightObservables.has(key)) {
      return this.inFlightObservables.get(key);
    } else if (fallback && fallback instanceof Observable) {
      this.inFlightObservables.set(key, new Subject());
      console.log(`%c Calling api for ${key}`, 'color: purple');
      return fallback.pipe(
        catchError(er => {
          this.inFlightObservables.delete(key);
          return of(er)
        }),
        tap(value => {
          this.set(key, value, maxAge);
        })
      )


    } else {
      return throwError('Requested key is not available in Cache');
    }

  }

  /**
   * Sets the value with key in the cache
   * Notifies all observers of the new value
   */
  set(key: string, value: any, maxAge: number = this.DEFAULT_MAX_AGE): void {
    this.cache.set(key, { value: value, expiry: Date.now() + maxAge });
    this.notifyInFlightObservers(key, value);
  }

  /**
   * Checks if the a key exists in cache
   */
  has(key: string): boolean {
    return this.cache.has(key);
  }

  /**
   * Publishes the value to all observers of the given
   * in progress observables if observers exist.
   */
  private notifyInFlightObservers(key: string, value: any): void {

    if (this.inFlightObservables.has(key)) {

      const inFlight = this.inFlightObservables.get(key);
      const observersCount = inFlight.observers.length;
      if (observersCount) {
        console.log(`%cNotifying ${inFlight.observers.length} flight subscribers for ${key}`, 'color: blue');
        inFlight.next(value);
      }
      inFlight.complete();
      this.inFlightObservables.delete(key);
    }
  }

  /**
   * Removes all elements from a cache
   */
  clearCache() {
    this.cache.clear()
  }

  /**
   * Checks if the key exists and   has not expired.
   */
  private hasValidCachedValue(key: string): boolean {
    if (this.cache.has(key)) {
      if (this.cache.get(key).expiry < Date.now()) {
        this.cache.delete(key);
        return false;
      }
      return true;
    } else {
      return false;
    }
  }
}

//other component     
//this.variable = this.cacheService.get(id, this.service.get(id))
//this.cacheService.get(id, this.service.get(id)).pipe(takeUntil(this.destroy)).subscribe(r => { })
                           

